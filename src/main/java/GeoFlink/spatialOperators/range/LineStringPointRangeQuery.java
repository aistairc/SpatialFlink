package GeoFlink.spatialOperators.range;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class LineStringPointRangeQuery extends RangeQuery<LineString, Point> {
    //--------------- Real-time - POINT - LineString -----------------//
    public DataStream<LineString> realtimeQuery(DataStream<LineString> lineStringStream, Point queryPoint, double queryRadius, UniformGrid uGrid, boolean approximateQuery) {

        Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);
        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);

        // Filtering out the linestrings which lie greater than queryRadius of the query point
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
                            distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(queryPoint, lineString);
                        }else{
                            distance = DistanceFunctions.getDistance(queryPoint, lineString);
                        }

                        if (distance <= queryRadius) {
                            collector.collect(lineString);
                        }
                        break;
                    }
                }

            }
        }).name("Real-time - POINT - LINESTRING");

        return rangeQueryNeighbours;
    }

    //--------------- Window-based - POINT - LineString -----------------//
    public DataStream<LineString> windowQuery(DataStream<LineString> lineStringStream, Point queryPoint, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery) {

        Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);
        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);

        DataStream<LineString> streamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString ls) {
                        return ls.timeStampMillisec;
                    }
                }).startNewChain();

        // Filtering out the linestrings which lie greater than queryRadius of the query point
        DataStream<LineString> filteredLineStrings = streamWithTsAndWm.flatMap(new RangeQuery.cellBasedLineStringFlatMap(neighboringCells));

        DataStream<LineString> rangeQueryNeighbours = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
            @Override
            public String getKey(LineString lineString) throws Exception {
                return lineString.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new WindowFunction<LineString, LineString, String, TimeWindow>() {
                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<LineString> pointIterator, Collector<LineString> neighbors) throws Exception {
                        for (LineString lineString : pointIterator) {
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
                                        distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(queryPoint, lineString);
                                    }else{
                                        distance = DistanceFunctions.getDistance(queryPoint, lineString);
                                    }

                                    if (distance <= queryRadius) {
                                        neighbors.collect(lineString);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }).name("Window-based - POINT - LINESTRING");

        return rangeQueryNeighbours;
    }

    //--------------- Window-based - POINT - LineString -----------------//
    public DataStream<LineString> queryIncremental(DataStream<LineString> lineStringStream, Point queryPoint, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery) {

        Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);
        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);

        DataStream<LineString> streamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString ls) {
                        return ls.timeStampMillisec;
                    }
                }).startNewChain();

        // Filtering out the linestrings which lie greater than queryRadius of the query point
        DataStream<LineString> filteredLineStrings = streamWithTsAndWm.flatMap(new RangeQuery.cellBasedLineStringFlatMap(neighboringCells));

        DataStream<LineString> rangeQueryNeighbours = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
            @Override
            public String getKey(LineString lineString) throws Exception {
                return lineString.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new RichWindowFunction<LineString, LineString, String, TimeWindow>() {

                    /**
                     * The ListState handle.
                     */
                    private transient ListState<LineString> queryOutputListState;

                    @Override
                    public void open(Configuration config) {
                        PojoTypeInfo<LineString> objTypeInfo = (PojoTypeInfo<LineString>) TypeInformation.of(LineString.class);

                        ListStateDescriptor<LineString> queryOutputStateDescriptor = new ListStateDescriptor<LineString>(
                                "queryOutputStateDescriptor",// state name
                                objTypeInfo);
                        //TypeInformation.of(new TypeHint<Point>() {})
                        this.queryOutputListState = getRuntimeContext().getListState(queryOutputStateDescriptor);
                    }

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<LineString> objIterator, Collector<LineString> neighbors) throws Exception {

                        List<LineString> nextWindowUsefulOutputFromPastWindow = new ArrayList<>();
                        // Access the list state - past output
                        for(LineString obj:queryOutputListState.get()){
                            neighbors.collect(obj);

                            // Storing objects useful for next window
                            if(obj.timeStampMillisec >= (timeWindow.getStart() + (slideStep * 1000))) {
                                nextWindowUsefulOutputFromPastWindow.add(obj);
                            }
                        }

                        // Clear the list state
                        queryOutputListState.clear();
                        // Populating the list state with the objects useful for next window
                        queryOutputListState.addAll(nextWindowUsefulOutputFromPastWindow);

                        for (LineString lineString : objIterator) {
                            // Check for Range Query only for new objects
                            if(lineString.timeStampMillisec >= (timeWindow.getEnd() - (slideStep * 1000))) {

                                int cellIDCounter = 0;
                                for (String lineStringGridID : lineString.gridIDsSet) {
                                    if (guaranteedNeighboringCells.contains(lineStringGridID)) {
                                        cellIDCounter++;
                                        // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                        if (cellIDCounter == lineString.gridIDsSet.size()) {
                                            neighbors.collect(lineString);
                                            queryOutputListState.add(lineString); // add new output useful for next window
                                        }
                                    } else { // candidate neighbors

                                        Double distance;
                                        if (approximateQuery) {
                                            distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(queryPoint, lineString);
                                        } else {
                                            distance = DistanceFunctions.getDistance(queryPoint, lineString);
                                        }

                                        if (distance <= queryRadius) {
                                            neighbors.collect(lineString);
                                            queryOutputListState.add(lineString); // add new output useful for next window
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }).name("Window-based - POINT - LINESTRING - Incremental");

        return rangeQueryNeighbours;
    }
}
