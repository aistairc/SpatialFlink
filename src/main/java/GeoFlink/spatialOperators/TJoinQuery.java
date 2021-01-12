package GeoFlink.spatialOperators;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.co.IntervalJoinOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

public class TJoinQuery implements Serializable {

    //--------------- Spatial Trajectory  JOIN QUERY -----------------//
    public static DataStream<Tuple2<Point, Point>> TSpatialJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double joinDistance, int windowSize, UniformGrid uGrid) {

        /*
        KeyedStream<Point, String> replicatedKeyedQueryStream = JoinQuery.getReplicatedQueryStream(queryPointStream, joinDistance, uGrid).keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.gridID;
            }
        });

        KeyedStream<Point, String> ordinaryKeyedQueryStream = ordinaryPointStream.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point q) throws Exception {
                return q.gridID;
            }});
         */

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> ordinaryStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {

                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> queryStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = getReplicatedQueryStream(queryStreamWithTsAndWm, joinDistance, uGrid);

        DataStream<Tuple2<Point, Point>> joinOutput = ordinaryStreamWithTsAndWm.join(replicatedQueryStream)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSize)))
                .apply(new JoinFunction<Point, Point, Tuple2<Point,Point>>() {
                    @Override
                    public Tuple2<Point, Point> join(Point p, Point q) {
                        //System.out.println(HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()));
                        if (HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()) <= joinDistance) {
                            return Tuple2.of(p, q);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                }).filter(new FilterFunction<Tuple2<Point, Point>>() {
            @Override
            public boolean filter(Tuple2<Point, Point> value) throws Exception {
                return (value.f1 != null); // removing null results
            }
        });

        // Join Output may contain multiple and/or null results
        // To generate output corresponding to latest timestamp of each trajectory, divide the tuples with respect to trajectory id rather than grid id as we are interested in one output per trajectory rather than one output per cell
        DataStream<Tuple2<Point, Point>> joinFilteredOutput = joinOutput.keyBy(new KeySelector<Tuple2<Point, Point>, String>() {  // Logic to remove multiple result per window, by returning only the latest result corresponding to a trajectory
            @Override
            public String getKey(Tuple2<Point, Point> e) throws Exception {
                return e.f0.objID; // keyBy Trajectory ID
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSize))).apply(new WindowFunction<Tuple2<Point, Point>, Tuple2<Point, Point>, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<Tuple2<Point, Point>> input, Collector<Tuple2<Point, Point>> out) throws Exception {

                // Step 1: Find the duplicate tuples
                // a) First element is guaranteed to be the same with the objID as "key" of apply function, as keyBy objID is used
                // b) Find duplicates in the tuple's second element creating a set and an array

                HashMap<String, Point> secondIDPoint = new HashMap<>();
                Point firstPoint = null;

                for (Tuple2<Point, Point> e :input) {

                    Point existingPoint = secondIDPoint.get(e.f1.objID);

                    if(existingPoint != null){     // If element is already in the set
                        Long oldTimestamp = existingPoint.timeStampMillisec;
                        if(e.f1.timeStampMillisec > oldTimestamp){ // if new timestamp is larger, replace the old element with new
                            secondIDPoint.replace(e.f1.objID, e.f1);
                        }
                    }
                    else { // insert new element if does not exist
                        secondIDPoint.put(e.f1.objID, e.f1);
                        firstPoint = e.f0;
                    }
                }

                // Collecting output
                for (Map.Entry<String, Point> entry : secondIDPoint.entrySet()){
                    out.collect(Tuple2.of(firstPoint, entry.getValue()));
                }
            }
        });

        return joinFilteredOutput;
    }


    //--------------- Spatial Trajectory  JOIN QUERY - Naive -----------------//
    public static DataStream<Tuple2<Point, Point>> TSpatialJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double joinDistance, int windowSize) {


        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> ordinaryStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {

                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> queryStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, Point>> joinOutput = ordinaryStreamWithTsAndWm.join(queryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return "1";
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return "1";
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSize)))
                .apply(new JoinFunction<Point, Point, Tuple2<Point,Point>>() {
                    @Override
                    public Tuple2<Point, Point> join(Point p, Point q) {
                        //System.out.println(HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()));
                        if (HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()) <= joinDistance) {
                            return Tuple2.of(p, q);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                }).filter(new FilterFunction<Tuple2<Point, Point>>() {
                    @Override
                    public boolean filter(Tuple2<Point, Point> value) throws Exception {
                        return (value.f1 != null); // removing null results
                    }
                });

        // Join Output may contain multiple and/or null results
        // To generate output corresponding to latest timestamp of each trajectory, divide the tuples with respect to trajectory id rather than grid id as we are interested in one output per trajectory rather than one output per cell
        DataStream<Tuple2<Point, Point>> joinFilteredOutput = joinOutput.keyBy(new KeySelector<Tuple2<Point, Point>, String>() {  // Logic to remove multiple result per window, by returning only the latest result corresponding to a trajectory
            @Override
            public String getKey(Tuple2<Point, Point> e) throws Exception {
                return e.f0.objID; // keyBy Trajectory ID
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSize))).apply(new WindowFunction<Tuple2<Point, Point>, Tuple2<Point, Point>, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<Tuple2<Point, Point>> input, Collector<Tuple2<Point, Point>> out) throws Exception {

                // Step 1: Find the duplicate tuples
                // a) First element is guaranteed to be the same with the objID as "key" of apply function, as keyBy objID is used
                // b) Find duplicates in the tuple's second element creating a set and an array

                HashMap<String, Point> secondIDPoint = new HashMap<>();
                Point firstPoint = null;

                for (Tuple2<Point, Point> e :input) {

                    Point existingPoint = secondIDPoint.get(e.f1.objID);

                    if(existingPoint != null){     // If element is already in the set
                        Long oldTimestamp = existingPoint.timeStampMillisec;
                        if(e.f1.timeStampMillisec > oldTimestamp){ // if new timestamp is larger, replace the old element with new
                            secondIDPoint.replace(e.f1.objID, e.f1);
                        }
                    }
                    else { // insert new element if does not exist
                        secondIDPoint.put(e.f1.objID, e.f1);
                        firstPoint = e.f0;
                    }
                }

                // Collecting output
                for (Map.Entry<String, Point> entry : secondIDPoint.entrySet()){
                    out.collect(Tuple2.of(firstPoint, entry.getValue()));
                }
            }
        });

        return joinFilteredOutput;
    }

    //Replicate Query Point Stream for each Neighbouring Grid ID
    public static DataStream<Point> getReplicatedQueryStream(DataStream<Point> queryPoints, double queryRadius, UniformGrid uGrid){

        return queryPoints.flatMap(new FlatMapFunction<Point, Point>() {
            @Override
            public void flatMap(Point queryPoint, Collector<Point> out) throws Exception {

                // Neighboring cells contain all the cells including Candidate cells, Guaranteed Cells and the query point cell itself
                HashSet<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);

                // Create duplicated query points
                for (String gridID: neighboringCells) {
                    Point p = new Point(queryPoint.objID, queryPoint.point.getX(), queryPoint.point.getY(), queryPoint.timeStampMillisec, gridID);
                    out.collect(p);
                }
            }
        });
    }
}
