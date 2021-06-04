package GeoFlink.spatialOperators.range;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LineStringPolygonRangeQuery extends RangeQuery<LineString, Polygon> {
    //--------------- Real-time - LINESTRING - POLYGON -----------------//
    public DataStream<LineString> realtimeQuery(DataStream<LineString> lineStringStream, Polygon queryPolygon, double queryRadius, UniformGrid uGrid, boolean approximateQuery) {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);
        Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(),candidateNeighboringCells.stream()).collect(Collectors.toSet());

        DataStream<LineString> filteredLineStrings = lineStringStream.flatMap(new RangeQuery.cellBasedLineStringFlatMap(neighboringCells)).startNewChain();

        DataStream<LineString> rangeQueryNeighbours = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
            @Override
            public String getKey(LineString lineString) throws Exception {
                return lineString.gridID;
            }
        }).flatMap(new FlatMapFunction<LineString, LineString>() {
            @Override
            public void flatMap(LineString lineString, Collector<LineString> collector) throws Exception {

                int cellIDCounter = 0;
                for (String lineStringGridID : lineString.gridIDsSet) {
                    if (guaranteedNeighboringCells.contains(lineStringGridID)){
                        cellIDCounter++;
                        // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                        if (cellIDCounter == lineString.gridIDsSet.size()) {
                            collector.collect(lineString);
                        }
                    }
                    else { // candidate neighbors
                        Double distance;
                        if(approximateQuery) {
                            distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryPolygon.boundingBox, lineString.boundingBox);
                        }else{
                            distance = DistanceFunctions.getDistance(queryPolygon, lineString);
                        }

                        if (distance <= queryRadius) {
                            collector.collect(lineString);
                        }
                        break;
                    }
                }

            }
        }).name("Real-time - LINESTRING - POLYGON");

        return rangeQueryNeighbours;
    }

    //--------------- WINDOW-based - LINESTRING - POLYGON -----------------//
    public DataStream<LineString> windowQuery(DataStream<LineString> lineStringStream, Polygon queryPolygon, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery) {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);
        Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(),candidateNeighboringCells.stream()).collect(Collectors.toSet());

        DataStream<LineString> streamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString ls) {
                        return ls.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> filteredLineStrings = streamWithTsAndWm.flatMap(new RangeQuery.cellBasedLineStringFlatMap(neighboringCells));

        DataStream<LineString> rangeQueryNeighbours = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
            @Override
            public String getKey(LineString lineString) throws Exception {
                return lineString.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new WindowFunction<LineString, LineString, String, TimeWindow>() {
                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<LineString> lineStringIterator, Collector<LineString> neighbors) throws Exception {

                        for (LineString lineString : lineStringIterator) {
                            int cellIDCounter = 0;
                            for (String lineStringGridID : lineString.gridIDsSet) {
                                if (guaranteedNeighboringCells.contains(lineStringGridID)){
                                    cellIDCounter++;
                                    // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                    if (cellIDCounter == lineString.gridIDsSet.size()) {
                                        neighbors.collect(lineString);
                                    }
                                }
                                else { // candidate neighbors
                                    Double distance;
                                    if(approximateQuery) {
                                        distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryPolygon.boundingBox, lineString.boundingBox);
                                    }else{
                                        distance = DistanceFunctions.getDistance(queryPolygon, lineString);
                                    }

                                    if (distance <= queryRadius) {
                                        neighbors.collect(lineString);
                                    }
                                    break;
                                }
                            }
                        }

                    }
                }).name("Window-based - LINESTRING - POLYGON");

        return rangeQueryNeighbours;
    }
}
