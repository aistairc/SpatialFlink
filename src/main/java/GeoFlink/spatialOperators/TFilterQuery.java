package GeoFlink.spatialOperators;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class TFilterQuery implements Serializable {

    //--------------- TrajIDFilter QUERY - Infinite Window -----------------//
    public static DataStream<Point> TIDSpatialFilterQuery(DataStream<Point> pointStream, Set<String> trajIDSet){

        return pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {

                //Date date = new Date();
                //System.out.println(date.getTime() - point.ingestionTime);
                if (trajIDSet.size() > 0)
                    return ((trajIDSet.contains(point.objID)));
                else
                    return true;
            }
        }).name("TrajIDFilterQuery").startNewChain();
    }

    //--------------- TrajIDFilter QUERY - Window-based -----------------//
    public static DataStream<Tuple2<String, LineString>> TIDSpatialFilterQuery(DataStream<Point> pointStream, Set<String> trajIDSet, int windowSize, int windowSlideStep){

        // Linkedlist for storing trajectory
        LinkedList<Point> objTraj = new LinkedList<>(); // Insertion complexity O(1)

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
        }).name("TrajIDFilterQueryWindowed").startNewChain();

        //filteredStream.print();

        DataStream<Tuple2<String, LineString>> windowedTrajectories = filteredStream.keyBy(new KeySelector<Point, String>() {

            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Point, Tuple2<String, LineString>, String, TimeWindow>() {
                    List<Coordinate> coordinateList = new LinkedList<>();
                    @Override
                    public void apply(String objID, TimeWindow timeWindow, Iterable<Point> pointIterator, Collector<Tuple2<String, LineString>> trajectory) throws Exception {
                        //objTraj.clear();
                        coordinateList.clear();
                        for (Point p : pointIterator) {
                            coordinateList.add(new Coordinate(p.point.getX(), p.point.getY()));
                            //objTraj.add(p);
                        }
                        LineString ls = new LineString(objID, coordinateList);

                        trajectory.collect(Tuple2.of(objID, ls));
                    }
                }).name("TrajIDFilterWindowedQuery");

        return  windowedTrajectories;
    }
}
