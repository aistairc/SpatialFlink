package GeoFlink.spatialOperators;

import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.io.Serializable;
import java.util.*;

public class TRangeQuery implements Serializable {

    //--------------- TSpatialRangeQuery Naive -----------------//
    public static DataStream<Point> TSpatialRangeQuery(Set<Polygon> polygonSet, DataStream<Point> pointStream){

        HashSet<String> polygonsGridCellIDs = new HashSet<>();
        // Making an integrated set of all the polygon's grid cell IDs
        for (Polygon poly: polygonSet) {
            polygonsGridCellIDs.addAll(poly.gridIDsSet);
        }

        // Perform keyBy to logically distribute streams by trajectoryID and then check if a point lies within a polygon or nor
        DataStream<Point> keyedStream =  pointStream.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
        }).filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point p) throws Exception {
                for (Polygon poly: polygonSet) {
                    if (poly.polygon.contains(p.point.getEnvelope())) // Polygon contains the point
                        return true;
                }
                return false; // Polygon does not contain the point
            }
        });

        return keyedStream;
    }


    //--------------- TSpatialRangeQuery - Realtime -----------------//
    public static DataStream<Point> TSpatialRangeQuery(DataStream<Point> pointStream, Set<Polygon> polygonSet){

        HashSet<String> polygonsGridCellIDs = new HashSet<>();
        // Making an integrated set of all the polygon's grid cell IDs
        for (Polygon poly: polygonSet) {
            polygonsGridCellIDs.addAll(poly.gridIDsSet);
        }

        // Filtering based on grid-cell ID
        DataStream<Point> filteredStream = pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point p) throws Exception {
                return ((polygonsGridCellIDs.contains(p.gridID)));
            }
        });

        // Perform keyBy to logically distribute streams by trajectoryID and then check if a point lies within a polygon or nor
        return filteredStream.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
        }).filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point p) throws Exception {
                for (Polygon poly: polygonSet) {
                    if (poly.polygon.contains(p.point.getEnvelope())) // Polygon contains the point
                        return true;
                }
                return false; // Polygon does not contain the point
            }
        });
    }


    /*
    //--------------- TSpatialRangeQuery - Window-based - outputs a trajectory consisting of only the points which lie within given region -----------------//
    public static DataStream<LineString> TSpatialRangeQuery(DataStream<Point> pointStream, Set<Polygon> polygonSet, int windowSize, int windowSlideStep){

        HashSet<String> polygonsGridCellIDs = new HashSet<>();

        // Making an integrated set of all the polygon's grid cell IDs
        for (Polygon poly: polygonSet) {
            polygonsGridCellIDs.addAll(poly.gridIDsSet);
        }

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        // Filtering based on grid-cell ID
        DataStream<Point> filteredStream = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point p) throws Exception {
                return ((polygonsGridCellIDs.contains(p.gridID)));
            }
        });

        // Perform keyBy to logically distribute streams by trajectoryID and then check if a point lies within a polygon or nor
        return filteredStream.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
        }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Point, LineString, String, TimeWindow>() {
                    List<Coordinate> coordinateList = new LinkedList<>();
                    @Override
                    public void apply(String objID, TimeWindow timeWindow, Iterable<Point> pointIterator, Collector<LineString> trajectory) throws Exception {
                        // Check all points in the window
                        coordinateList.clear();
                        for (Point p : pointIterator) {
                            for (Polygon poly: polygonSet) {
                                if (poly.polygon.contains(p.point.getEnvelope())) { // Polygon contains the point
                                    coordinateList.add(new Coordinate(p.point.getX(), p.point.getY()));
                                    break;
                                }
                            }
                        }
                        LineString ls = new LineString(objID, coordinateList);
                        trajectory.collect(ls);
                    }
                });
    }
    */

    //--------------- TSpatialRangeQuery - Window-based - outputs a trajectory consisting of a complete sub-trajectory if any of its point lie within given region -----------------//
    public static DataStream<LineString> TSpatialRangeQuery(DataStream<Point> pointStream, Set<Polygon> polygonSet, int windowSize, int windowSlideStep, int allowedLateness){

        HashSet<String> polygonsGridCellIDs = new HashSet<>();

        // Making an integrated set of all the polygon's grid cell IDs
        for (Polygon poly: polygonSet) {
            polygonsGridCellIDs.addAll(poly.gridIDsSet);
        }

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        // Filtering based on grid-cell ID
        DataStream<Point> filteredStream = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point p) throws Exception {
                return ((polygonsGridCellIDs.contains(p.gridID)));
            }
        });

        // Generating window-based unique trajIDs
        DataStream<String> trajIDStream = filteredStream.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep))).apply(new WindowFunction<Point, String, String, TimeWindow>() {
            List<Coordinate> coordinateList = new LinkedList<>();
            @Override
            public void apply(String objID, TimeWindow timeWindow, Iterable<Point> pointIterator, Collector<String> output) throws Exception {
                output.collect(objID);
            }
        });

        // Joining the original stream with the trajID stream - output contains only trajectories within given range
        DataStream<Point> joinedStream = pointStreamWithTsAndWm.join(trajIDStream).where(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
                }).equalTo(new KeySelector<String, String>() {
            @Override
            public String getKey(String trajID) throws Exception {
                return trajID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new JoinFunction<Point, String, Point>() {
                    @Override
                    public Point join(Point p, String trajID) {
                        return p;
                    }
                });

        // Construct window based sub-trajectories
        return joinedStream.keyBy(new KeySelector<Point, String>() {
               @Override
               public String getKey(Point p) throws Exception {
                   return p.objID;
               }
               }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                 .apply(new WindowFunction<Point, LineString, String, TimeWindow>() {
                    List<Coordinate> coordinateList = new LinkedList<>();
                    @Override
                    public void apply(String objID, TimeWindow timeWindow, Iterable<Point> pointIterator, Collector<LineString> trajectory) throws Exception {
                        coordinateList.clear();
                        for (Point p : pointIterator) {
                            coordinateList.add(new Coordinate(p.point.getX(), p.point.getY()));
                            }
                        LineString ls = new LineString(objID, coordinateList);
                        trajectory.collect(ls);
                        }
                    });
    }
}
