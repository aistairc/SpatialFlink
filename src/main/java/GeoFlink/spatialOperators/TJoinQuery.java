package GeoFlink.spatialOperators;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.co.IntervalJoinOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.io.Serializable;
import java.util.*;

public class TJoinQuery implements Serializable {

    //--------------- Spatial Trajectory  JOIN QUERY - Real Time -----------------//
    public static DataStream<Tuple2<Point, Point>> TSpatialJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double joinRadius, int omegaJoinDurationSeconds, int allowedLateness, UniformGrid uGrid) {

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> ordinaryStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Point> queryStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Point> replicatedQueryStream = getReplicatedQueryStream(queryStreamWithTsAndWm, joinRadius, uGrid);

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
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds), Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Point, Point, Tuple2<Point,Point>>() {
                    @Override
                    public Tuple2<Point, Point> join(Point p, Point q) {
                        //System.out.println(HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()));
                        if (DistanceFunctions.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()) <= joinRadius) {
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
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds), Time.seconds(omegaJoinDurationSeconds))).apply(new WindowFunction<Tuple2<Point, Point>, Tuple2<Point, Point>, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<Tuple2<Point, Point>> input, Collector<Tuple2<Point, Point>> out) throws Exception {

                // Step 1: Find the duplicate tuples
                // a) First element is guaranteed to be the same with the objID as "key" of apply function, as keyBy objID is used
                // b) Find duplicates in the tuple's second element creating a set and an array

                HashMap<String, Point> secondIDPointMap = new HashMap<>();
                Point firstPoint = null;
                Boolean firstPointSet = false;

                for (Tuple2<Point, Point> e :input) {

                    // Setting the first pair point, if not already set
                    if(!firstPointSet){
                        firstPoint = e.f0;
                        firstPointSet = true;
                    }

                    Point existingPoint = secondIDPointMap.get(e.f1.objID);
                    if(existingPoint != null){     // If element is already in the set
                        Long oldTimestamp = existingPoint.timeStampMillisec;
                        if(e.f1.timeStampMillisec > oldTimestamp){ // if new timestamp is larger, replace the old element with new
                            secondIDPointMap.replace(e.f1.objID, e.f1);
                        }
                    }
                    else { // insert new element if does not exist
                        secondIDPointMap.put(e.f1.objID, e.f1);
                    }
                }

                // Collecting output
                for (Map.Entry<String, Point> entry : secondIDPointMap.entrySet()){
                    out.collect(Tuple2.of(firstPoint, entry.getValue()));
                }
            }
        });

        return joinFilteredOutput;
    }

    //--------------- Spatial Trajectory JOIN QUERY - Single Stream - Real Time -----------------//
    public static DataStream<Tuple2<Point, Point>> TSpatialJoinQuery(DataStream<Point> pointStream, double joinDistance, int omegaJoinDurationSeconds, int allowedLateness, UniformGrid uGrid) {

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> ordinaryStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Point> replicatedQueryStream = getReplicatedQueryStream(ordinaryStreamWithTsAndWm, joinDistance, uGrid);

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
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds), Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Point, Point, Tuple2<Point,Point>>() {
                    @Override
                    public Tuple2<Point, Point> join(Point p, Point q) {
                        if(p.objID != q.objID) {// No need to join a trajectory with itself
                            //System.out.println(HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()));
                            if (DistanceFunctions.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()) <= joinDistance) {
                                return Tuple2.of(p, q);
                            } else {
                                return Tuple2.of(null, null);
                            }
                        }else {
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
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds), Time.seconds(omegaJoinDurationSeconds))).apply(new WindowFunction<Tuple2<Point, Point>, Tuple2<Point, Point>, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<Tuple2<Point, Point>> input, Collector<Tuple2<Point, Point>> out) throws Exception {

                // Step 1: Find the duplicate tuples
                // a) First element is guaranteed to be the same with the objID as "key" of apply function, as keyBy objID is used
                // b) Find duplicates in the tuple's second element creating a set and an array

                HashMap<String, Point> secondIDPointMap = new HashMap<>();
                Point firstPoint = null;
                Boolean firstPointSet = false;

                for (Tuple2<Point, Point> e :input) {

                    // Setting the first pair point, if not already set
                    if(!firstPointSet){
                        firstPoint = e.f0;
                        firstPointSet = true;
                    }

                    Point existingPoint = secondIDPointMap.get(e.f1.objID);
                    if(existingPoint != null){     // If element is already in the set
                        Long oldTimestamp = existingPoint.timeStampMillisec;
                        if(e.f1.timeStampMillisec > oldTimestamp){ // if new timestamp is larger, replace the old element with new
                            secondIDPointMap.replace(e.f1.objID, e.f1);
                        }
                    }
                    else { // insert new element if does not exist
                        secondIDPointMap.put(e.f1.objID, e.f1);
                    }
                }

                // Collecting output
                for (Map.Entry<String, Point> entry : secondIDPointMap.entrySet()){
                    out.collect(Tuple2.of(firstPoint, entry.getValue()));
                }
            }
        });

        return joinFilteredOutput;
    }

    //--------------- Spatial Trajectory  JOIN QUERY - Window Based -----------------//
    public static DataStream<Tuple2<LineString, LineString>> TSpatialJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double joinDistance, int windowSize, int slideStep, int allowedLateness, UniformGrid uGrid) {

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> ordinaryStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Point> queryStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        // Generation of ordinary stream trajectories
        DataStream<LineString> ordinaryStreamTrajectories = ordinaryStreamWithTsAndWm.keyBy(new trajIDKeySelector())
                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep))).apply(new GenerateWindowedTrajectory());

        // Generation of query stream trajectories
        DataStream<LineString> queryStreamTrajectories = queryStreamWithTsAndWm.keyBy(new trajIDKeySelector())
                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep))).apply(new GenerateWindowedTrajectory());

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
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Point, Point, Tuple2<Point,Point>>() {
                    @Override
                    public Tuple2<Point, Point> join(Point p, Point q) {
                        //System.out.println(HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()));
                        if (DistanceFunctions.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()) <= joinDistance) {
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
        DataStream<Tuple2<Point, Point>> joinedFilteredOutput = joinOutput.keyBy(new KeySelector<Tuple2<Point, Point>, String>() {  // Logic to remove multiple result per window, by returning only the latest result corresponding to a trajectory
            @Override
            public String getKey(Tuple2<Point, Point> e) throws Exception {
                return e.f0.objID; // keyBy Trajectory ID
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep))).apply(new WindowFunction<Tuple2<Point, Point>, Tuple2<Point, Point>, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<Tuple2<Point, Point>> input, Collector<Tuple2<Point, Point>> out) throws Exception {

                // Step 1: Find the duplicate tuples
                // a) First element is guaranteed to be the same with the objID as "key" of apply function, as keyBy objID is used
                // b) Find duplicates in the tuple's second element creating a set and an array

                HashMap<String, Point> secondIDPointMap = new HashMap<>();
                Point firstPoint = null;
                Boolean firstPointSet = false;

                for (Tuple2<Point, Point> e :input) {

                    // Setting the first pair point, if not already set
                    if(!firstPointSet){
                        firstPoint = e.f0;
                        firstPointSet = true;
                    }

                    Point existingPoint = secondIDPointMap.get(e.f1.objID);
                    if(existingPoint != null){     // If element is already in the set
                        Long oldTimestamp = existingPoint.timeStampMillisec;
                        if(e.f1.timeStampMillisec > oldTimestamp){ // if new timestamp is larger, replace the old element with new
                            secondIDPointMap.replace(e.f1.objID, e.f1);
                        }
                    }
                    else { // insert new element if does not exist
                        secondIDPointMap.put(e.f1.objID, e.f1);
                    }
                }

                // Collecting output
                for (Map.Entry<String, Point> entry : secondIDPointMap.entrySet()){
                    out.collect(Tuple2.of(firstPoint, entry.getValue()));
                }
            }
        });

        //joinedFilteredOutput.print();

        // Converting point to trajectories
        DataStream<Tuple2<LineString, Point>> joinedLineStringPointOutput = joinedFilteredOutput.join(ordinaryStreamTrajectories).where(new KeySelector<Tuple2<Point, Point>, String>() {
            @Override
            public String getKey(Tuple2<Point, Point> e) throws Exception {
                return e.f0.objID;
            }
        }).equalTo(new KeySelector<LineString, String>() {
            @Override
            public String getKey(LineString ls) throws Exception {
                return ls.objID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Tuple2<Point, Point>, LineString, Tuple2<LineString, Point>>() {
                    @Override
                    public Tuple2<LineString, Point> join(Tuple2<Point, Point> pointPointTuple, LineString ls) throws Exception {
                        return Tuple2.of(ls, pointPointTuple.f1);
                    }}).filter(new FilterFunction<Tuple2<LineString, Point>>() {
            @Override
            public boolean filter(Tuple2<LineString, Point> lineStringPointTuple2) throws Exception {
                return (lineStringPointTuple2.f0 != null); // removing null results
            }
        });


        // Converting point to trajectories
        return joinedLineStringPointOutput.join(queryStreamTrajectories).where(new KeySelector<Tuple2<LineString, Point>, String>() {
            @Override
            public String getKey(Tuple2<LineString, Point> e) throws Exception {
                return e.f1.objID;
            }
        }).equalTo(new KeySelector<LineString, String>() {
            @Override
            public String getKey(LineString ls) throws Exception {
                return ls.objID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Tuple2<LineString, Point>, LineString, Tuple2<LineString, LineString>>() {
                    @Override
                    public Tuple2<LineString, LineString> join(Tuple2<LineString, Point> pointPointTuple, LineString ls) throws Exception {
                        return Tuple2.of(pointPointTuple.f0, ls);
                    }}).filter(new FilterFunction<Tuple2<LineString, LineString>>() {
                    @Override
                    public boolean filter(Tuple2<LineString, LineString> lineStringPointTuple2) throws Exception {
                        return (lineStringPointTuple2.f1 != null); // removing null results
                    }
                });
    }

    //--------------- Spatial Trajectory  JOIN QUERY - Naive -----------------//
    public static DataStream<Tuple2<Point, Point>> TSpatialJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double joinRadius, int omegaJoinDurationSeconds, int allowedLateness) {

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> ordinaryStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Point> queryStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

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
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Point, Point, Tuple2<Point,Point>>() {
                    @Override
                    public Tuple2<Point, Point> join(Point p, Point q) {
                        //System.out.println(HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()));
                        if (DistanceFunctions.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()) <= joinRadius) {
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

        //return joinOutput;

        // Join Output may contain multiple and/or null results
        // To generate output corresponding to latest timestamp of each trajectory, divide the tuples with respect to trajectory id rather than grid id as we are interested in one output per trajectory rather than one output per cell
        DataStream<Tuple2<Point, Point>> joinFilteredOutput = joinOutput.keyBy(new KeySelector<Tuple2<Point, Point>, String>() {  // Logic to remove multiple result per window, by returning only the latest result corresponding to a trajectory
            @Override
            public String getKey(Tuple2<Point, Point> e) throws Exception {
                return e.f0.objID; // keyBy Trajectory ID
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds))).apply(new WindowFunction<Tuple2<Point, Point>, Tuple2<Point, Point>, String, TimeWindow>() {
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

    // User Defined Classes
    // Key selector
    public static class trajIDKeySelector implements KeySelector<Point,String> {
        @Override
        public String getKey(Point p) throws Exception {
            return p.objID; // trajectory id
        }
    }

    public static class GenerateWindowedTrajectory extends RichWindowFunction<Point, LineString, String, TimeWindow> {

        //ctor
        public  GenerateWindowedTrajectory() {};

        @Override
        public void apply(String trajID, TimeWindow timeWindow, Iterable<Point> input, Collector<LineString> trajectory) throws Exception {

            List<Coordinate> coordinateList = new LinkedList<>();
            HashSet<String> gridIDsSet = new HashSet<>();

            coordinateList.clear();
            gridIDsSet.clear();

            for (Point p : input) {
                coordinateList.add(new Coordinate(p.point.getX(), p.point.getY()));
                gridIDsSet.add(p.gridID);
            }

            if (coordinateList.size() > 1 ) { // At least two points are required for a lineString construction
                LineString ls = new LineString(trajID, coordinateList, gridIDsSet);

                if (ls != null) {
                    trajectory.collect(ls);
                }
            }
        }
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
