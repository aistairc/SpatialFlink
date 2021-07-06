package GeoFlink.spatialOperators.tJoin;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.SpatialOperator;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.io.Serializable;
import java.util.*;

public abstract class TJoinQuery<T extends SpatialObject, K extends SpatialObject> extends SpatialOperator implements Serializable {
    private QueryConfiguration queryConfiguration;
    private SpatialIndex spatialIndex;
    static long dCounter = 0;

    public QueryConfiguration getQueryConfiguration() {
        return queryConfiguration;
    }

    public void setQueryConfiguration(QueryConfiguration queryConfiguration) {
        this.queryConfiguration = queryConfiguration;
    }

    public SpatialIndex getSpatialIndex() {
        return spatialIndex;
    }

    public void setSpatialIndex(SpatialIndex spatialIndex) {
        this.spatialIndex = spatialIndex;
    }

    public void initializeKNNQuery(QueryConfiguration conf, SpatialIndex index){
        this.setQueryConfiguration(conf);
        this.setSpatialIndex(index);
    }

    public abstract DataStream<?> run(DataStream<K> ordinaryStream, DataStream<K> queryStream, double joinDistance);

    public static DataStream<Tuple2<Point, Point>> realTimeJoinNaive(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double joinDistance, int omegaJoinDurationSeconds, int allowedLateness) {


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
    protected class trajIDKeySelector implements KeySelector<Point,String> {
        @Override
        public String getKey(Point p) throws Exception {
            return p.objID; // trajectory id
        }
    }

    protected class GenerateWindowedTrajectory extends RichWindowFunction<Point, LineString, String, TimeWindow> {

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
    protected DataStream<Point> getReplicatedQueryStream(DataStream<Point> queryPoints, double queryRadius, UniformGrid uGrid){

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


    // TimeWindow Trigger
    protected class realTimeWindowTrigger extends Trigger<CoGroupedStreams.TaggedUnion<Point, Point>, TimeWindow> {
        @Override
        public TriggerResult onElement(CoGroupedStreams.TaggedUnion<Point, Point> pointPointTaggedUnion, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
            //return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }

    // TimeWindow Trigger
    protected class realTimeWindowPointPointTrigger extends Trigger<Tuple2<Point, Point>, TimeWindow> {
        @Override
        public TriggerResult onElement(Tuple2<Point, Point> pointPointTaggedUnion, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
            //return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }
}
