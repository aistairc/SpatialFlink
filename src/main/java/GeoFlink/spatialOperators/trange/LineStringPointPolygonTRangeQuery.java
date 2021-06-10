package GeoFlink.spatialOperators.trange;

import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
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

public class LineStringPointPolygonTRangeQuery extends TRangeQuery<LineString, Point, Polygon> {
    public LineStringPointPolygonTRangeQuery(QueryConfiguration conf) {
        super.initializeTRangeQuery(conf);
    }

    public DataStream<LineString> run(DataStream<Point> pointStream, Set<Polygon> polygonSet) {
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        //--------------- Real-time - POINT - POINT - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            throw new IllegalArgumentException("Not yet support");
        }

        //--------------- Window-based - LINESTRING - POINT - POLYGON -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int windowSlideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(pointStream, polygonSet, windowSize, windowSlideStep, allowedLateness);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
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