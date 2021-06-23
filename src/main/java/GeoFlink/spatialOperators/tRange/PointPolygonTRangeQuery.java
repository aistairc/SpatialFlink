
package GeoFlink.spatialOperators.tRange;

import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class PointPolygonTRangeQuery extends TRangeQuery<Point, Polygon> {
    public PointPolygonTRangeQuery(QueryConfiguration conf) {
        super.initializeTRangeQuery(conf);
    }

    public DataStream<? extends SpatialObject> run(DataStream<Point> pointStream, Set<Polygon> polygonSet) {
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        //--------------- Real-time - POINT - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            return realTime(pointStream, polygonSet);
        }

        //--------------- Window-based - POINT - POLYGON -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int windowSlideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(pointStream, polygonSet, windowSize, windowSlideStep, allowedLateness);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Point> realTime(DataStream<Point> pointStream, Set<Polygon> polygonSet) {

        HashSet<String> polygonsGridCellIDs = new HashSet<>();
        // Making an integrated set of all the polygon's grid cell IDs
        for (Polygon poly : polygonSet) {
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
                for (Polygon poly : polygonSet) {
                    //counter += 1;
                    //System.out.println("counter " +  counter);
                    if (poly.polygon.contains(p.point.getEnvelope())) // Polygon contains the point
                        return true;
                }
                return false; // Polygon does not contain the point
            }
        });
    }

    // WINDOW BASED
    private DataStream<LineString> windowBased(DataStream<Point> pointStream, Set<Polygon> polygonSet, int windowSize, int windowSlideStep, int allowedLateness){

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

        // Identifying trajectories' point which fulfill range query criteria
        DataStream<String> trajIDStream = filteredStream.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep))).apply(new WindowFunction<Point, String, String, TimeWindow>() {
            List<Coordinate> coordinateList = new LinkedList<>();
            @Override
            public void apply(String objID, TimeWindow timeWindow, Iterable<Point> pointIterator, Collector<String> output) throws Exception {
                // all the points in the pointIterator belong to one trajectory, so even if a single point fulfills range query criteria, the whole trajectory does
                for (Point p:pointIterator) {
                    for (Polygon poly : polygonSet) {
                        if (poly.polygon.contains(p.point.getEnvelope())) { // Polygon contains the point
                            output.collect(objID);
                            return;
                        }
                    }
                }
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