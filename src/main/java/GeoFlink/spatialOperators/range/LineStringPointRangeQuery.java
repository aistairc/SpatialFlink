package GeoFlink.spatialOperators.range;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LineStringPointRangeQuery extends RangeQuery<LineString, Point> {
    public LineStringPointRangeQuery(QueryConfiguration conf, SpatialIndex index){
        super.initializeRangeQuery(conf, index);
    }

    public DataStream<LineString> run(DataStream<LineString> lineStringStream, Set<Point> queryPointSet, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

        //--------------- Real-time - POINT - LineString -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {

            //Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, ((Point[])queryPointSet.toArray())[0]);
            //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((Point[])queryPointSet.toArray())[0].gridID);

            HashSet<String> neighboringCells = new HashSet<>();
            HashSet<String> guaranteedNeighboringCells = new HashSet<>();

            for(Point point:queryPointSet) {
                neighboringCells.addAll(uGrid.getNeighboringCells(queryRadius, point));
                guaranteedNeighboringCells.addAll(uGrid.getGuaranteedNeighboringCells(queryRadius, point.gridID));
            }

            // Filtering out the linestrings which lie greater than queryRadius of the query point
            DataStream<LineString> filteredLineStrings = lineStringStream.flatMap(new CellBasedLineStringFlatMap(neighboringCells)).startNewChain();

            return filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
                @Override
                public String getKey(LineString lineString) throws Exception {
                    return lineString.gridID;
                }
            }).flatMap(new FlatMapFunction<LineString, LineString>() {
                @Override
                public void flatMap(LineString lineString, Collector<LineString> collector) throws Exception {

                    int cellIDCounter = 0;
                    for (String lineStringGridID : lineString.gridIDsSet) {
                        if (guaranteedNeighboringCells.contains(lineStringGridID)) {
                            cellIDCounter++;
                            // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                            if (cellIDCounter == lineString.gridIDsSet.size()) {
                                collector.collect(lineString);
                            }
                        } else { // candidate neighbors

                            double distance;

                            for(Point point:queryPointSet) {

                                if (approximateQuery) {
                                    distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(point, lineString);
                                } else {
                                    distance = DistanceFunctions.getDistance(point, lineString);
                                }

                                if (distance <= queryRadius) {
                                    collector.collect(lineString);
                                    break;
                                }

                                /*
                                if (approximateQuery) {
                                    distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(((Point[]) queryPointSet.toArray())[0], lineString);
                                } else {
                                    distance = DistanceFunctions.getDistance(((Point[]) queryPointSet.toArray())[0], lineString);
                                }

                                if (distance <= queryRadius) {
                                    collector.collect(lineString);
                                }*/
                            }
                            break;
                        }
                    }

                }
            }).name("Real-time - POINT - LINESTRING");
        }
        //--------------- Window-based - POINT - LineString -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();

            //Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, ((Point[])queryPointSet.toArray())[0]);
            //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((Point[])queryPointSet.toArray())[0].gridID);

            HashSet<String> neighboringCells = new HashSet<>();
            HashSet<String> guaranteedNeighboringCells = new HashSet<>();

            for(Point point:queryPointSet) {
                neighboringCells.addAll(uGrid.getNeighboringCells(queryRadius, point));
                guaranteedNeighboringCells.addAll(uGrid.getGuaranteedNeighboringCells(queryRadius, point.gridID));
            }

            DataStream<LineString> streamWithTsAndWm =
                    lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(LineString ls) {
                            return ls.timeStampMillisec;
                        }
                    }).startNewChain();

            // Filtering out the linestrings which lie greater than queryRadius of the query point
            DataStream<LineString> filteredLineStrings = streamWithTsAndWm.flatMap(new CellBasedLineStringFlatMap(neighboringCells));

            return filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
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
                                    if (guaranteedNeighboringCells.contains(lineStringGridID)) {
                                        cellIDCounter++;
                                        // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                        if (cellIDCounter == lineString.gridIDsSet.size()) {
                                            neighbors.collect(lineString);
                                        }
                                    } else { // candidate neighbors

                                        double distance;

                                        for(Point point:queryPointSet) {

                                            if (approximateQuery) {
                                                distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(point, lineString);
                                            } else {
                                                distance = DistanceFunctions.getDistance(point, lineString);
                                            }

                                            if (distance <= queryRadius) {
                                                neighbors.collect(lineString);
                                                break;
                                            }

                                /*
                                if (approximateQuery) {
                                    distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(((Point[]) queryPointSet.toArray())[0], lineString);
                                } else {
                                    distance = DistanceFunctions.getDistance(((Point[]) queryPointSet.toArray())[0], lineString);
                                }

                                if (distance <= queryRadius) {
                                    collector.collect(lineString);
                                }*/

                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }).name("Window-based - POINT - LINESTRING");
        } else {
            throw new IllegalArgumentException("Not yet support");
        }
    }
}
