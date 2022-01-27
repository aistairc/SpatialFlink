package GeoFlink.spatialOperators.join;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class PointPointJoinQuery extends JoinQuery<Point, Point> {
    public PointPointJoinQuery(QueryConfiguration conf, SpatialIndex index1, SpatialIndex index2){
        super.initializeJoinQuery(conf, index1, index2);
    }

    public DataStream<Tuple2<Point, Point>> run(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex1();
        UniformGrid qGrid = (UniformGrid) this.getSpatialIndex2();

        //--------------- Real-time - POINT - POINT -----------------//
        if(this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            //return realTime(ordinaryPointStream, queryPointStream, queryRadius, omegaJoinDurationSeconds, uGrid, qGrid, allowedLateness, approximateQuery);
            return windowBased(ordinaryPointStream, queryPointStream, queryRadius, omegaJoinDurationSeconds, omegaJoinDurationSeconds, uGrid, qGrid, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - POINT - POINT -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(ordinaryPointStream, queryPointStream, queryRadius, windowSize, slideStep, uGrid, qGrid, allowedLateness, approximateQuery);
        }

        //--------------- Real-time - POINT - POINT -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.RealTimeNaive) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            //return realTime(ordinaryPointStream, queryPointStream, queryRadius, omegaJoinDurationSeconds, uGrid, qGrid, allowedLateness, approximateQuery);
            return realTimeNaive(ordinaryPointStream, queryPointStream, queryRadius, omegaJoinDurationSeconds, omegaJoinDurationSeconds, uGrid, qGrid, allowedLateness, approximateQuery);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    /*
    private DataStream<Tuple2<Point, Point>> realTime(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double queryRadius, int omegaJoinDurationSeconds, UniformGrid uGrid, UniformGrid qGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedPointQueryStream(queryPointStream, queryRadius, qGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, Point>> joinOutput = pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
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
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Point, Point, Tuple2<Point,Point>>() {
                    @Override
                    public Tuple2<Point, Point> join(Point p, Point q) {

                        if (approximateQuery) { // all the candidate neighbors are sent to output
                            return Tuple2.of(p, q);
                        } else {

                            if (DistanceFunctions.getDistance(p, q) <= queryRadius) {
                                return Tuple2.of(p, q);
                            } else {
                                return Tuple2.of(null, null);
                            }

                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Point, Point>>() {
            @Override
            public boolean filter(Tuple2<Point, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }
     */


    // WINDOW BASED
    private DataStream<Tuple2<Point, Point>> windowBased(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double queryRadius, int windowSize, int slideStep, UniformGrid uGrid, UniformGrid qGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedPointQueryStream(queryPointStream, queryRadius, qGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, Point>> joinOutput = pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
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

                        if (approximateQuery) { // all the candidate neighbors are sent to output
                            return Tuple2.of(p, q);
                        } else {

                            if (DistanceFunctions.getDistance(p, q) <= queryRadius) {
                                return Tuple2.of(p, q);
                            } else {
                                return Tuple2.of(null, null);
                            }
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Point, Point>>() {
            @Override
            public boolean filter(Tuple2<Point, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    // Real-time Naive
    private DataStream<Tuple2<Point, Point>> realTimeNaive(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double queryRadius, int windowSize, int slideStep, UniformGrid uGrid, UniformGrid qGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> queryStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, Point>> joinOutput = pointStreamWithTsAndWm.join(queryStreamWithTsAndWm)
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
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Point, Point, Tuple2<Point,Point>>() {
                    @Override
                    public Tuple2<Point, Point> join(Point p, Point q) {

                        if (approximateQuery) { // all the candidate neighbors are sent to output
                            return Tuple2.of(p, q);
                        } else {

                            if (DistanceFunctions.getDistance(p, q) <= queryRadius) {
                                return Tuple2.of(p, q);
                            } else {
                                return Tuple2.of(null, null);
                            }
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Point, Point>>() {
            @Override
            public boolean filter(Tuple2<Point, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

}
