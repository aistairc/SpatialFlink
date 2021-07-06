//package GeoFlink.SpatialOperator;

package GeoFlink.spatialOperators.tFilter;

import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class PointTFilterQuery extends TFilterQuery<Point> {
    public PointTFilterQuery(QueryConfiguration conf) {
        super.initializeKNNQuery(conf);
    }

    public DataStream<? extends SpatialObject> run(DataStream<Point> pointStream, Set<String> trajIDSet) {

        //--------------- Real-time - POINT -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            return realTime(pointStream, trajIDSet);
        }

        //--------------- Window-based - POINT -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int windowSlideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(pointStream, trajIDSet, windowSize, windowSlideStep);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Point> realTime(DataStream<Point> pointStream, Set<String> trajIDSet){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        return pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {

                //Date date = new Date();
                //System.out.println(date.getTime() - point.ingestionTime);
                if (trajIDSet.size() > 0)
                    return ((trajIDSet.contains(point.objID)));
                else
                    return true;
            }
        }).name("TrajIDFilterQuery");
    }

    // WINDOW BASED
    private DataStream<LineString> windowBased(DataStream<Point> pointStream, Set<String> trajIDSet, int windowSize, int windowSlideStep){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Point> filteredStream = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                if (trajIDSet.size() > 0)
                    return ((trajIDSet.contains(point.objID)));
                else
                    return true;
            }
        }).name("TrajIDFilterQueryWindowed");

        //filteredStream.print();
        DataStream<LineString> windowedTrajectories = filteredStream.keyBy(new KeySelector<Point, String>() {

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
                }).name("TrajIDFilterWindowedQuery");

        return  windowedTrajectories;
    }
}
