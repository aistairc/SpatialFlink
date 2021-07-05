package GeoFlink.spatialOperators.tJoin;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class PointPointTJoinQuery extends TJoinQuery<Point, Point> {
    public PointPointTJoinQuery(QueryConfiguration conf, SpatialIndex index) {
        super.initializeKNNQuery(conf, index);
    }

    public DataStream<?> run(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double joinDistance) {
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();
        int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

        //--------------- Real-time - LINESTRING - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            return realTime(ordinaryPointStream, queryPointStream, joinDistance, omegaJoinDurationSeconds, allowedLateness, uGrid);
        }

        //--------------- Window-based - LINESTRING - POLYGON -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep  = this.getQueryConfiguration().getSlideStep();
            return windowBased(ordinaryPointStream, queryPointStream, joinDistance, windowSize, slideStep, allowedLateness, uGrid);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    public DataStream<Tuple2<Point, Point>> runSingle(DataStream<Point> pointStream, double joinDistance) {
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();
        int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

        //--------------- Real-time - LINESTRING - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            return commonSingle(pointStream, joinDistance, omegaJoinDurationSeconds, allowedLateness, uGrid);
        }

        //--------------- Window-based - LINESTRING - POLYGON -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            return commonSingle(pointStream, joinDistance, omegaJoinDurationSeconds, allowedLateness, uGrid);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple2<Point, Point>> realTime(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double joinDistance, int omegaJoinDurationSeconds, int allowedLateness, UniformGrid uGrid) {

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
                });//;startNewChain();

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
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds), Time.seconds(omegaJoinDurationSeconds)))
                .trigger(new TJoinQuery.realTimeWindowTrigger())
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
        DataStream<Tuple2<Point, Point>> joinFilteredOutput = joinOutput.keyBy(new KeySelector<Tuple2<Point, Point>, String>() {  // Logic to remove multiple result per window, by returning only the latest result corresponding to a trajectory
            @Override
            public String getKey(Tuple2<Point, Point> e) throws Exception {
                return e.f0.objID; // keyBy Trajectory ID
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds), Time.seconds(omegaJoinDurationSeconds)))
                .trigger(new TJoinQuery.realTimeWindowPointPointTrigger())
                .apply(new WindowFunction<Tuple2<Point, Point>, Tuple2<Point, Point>, String, TimeWindow>() {
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

    // WINDOW BASED
    private DataStream<Tuple2<LineString, LineString>> windowBased(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double joinDistance, int windowSize, int slideStep, int allowedLateness, UniformGrid uGrid) {

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
        DataStream<LineString> ordinaryStreamTrajectories = ordinaryStreamWithTsAndWm.keyBy(new TJoinQuery.trajIDKeySelector())
                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep))).apply(new TJoinQuery.GenerateWindowedTrajectory());

        // Generation of query stream trajectories
        DataStream<LineString> queryStreamTrajectories = queryStreamWithTsAndWm.keyBy(new TJoinQuery.trajIDKeySelector())
                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep))).apply(new TJoinQuery.GenerateWindowedTrajectory());

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

    // Single
    private DataStream<Tuple2<Point, Point>> commonSingle(DataStream<Point> pointStream, double joinDistance, int omegaJoinDurationSeconds, int allowedLateness, UniformGrid uGrid) {

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
}
