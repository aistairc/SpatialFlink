package GeoFlink.spatialOperators.join;

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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class PointLineStringJoinQuery extends JoinQuery<Point, LineString> {
    public PointLineStringJoinQuery(QueryConfiguration conf, SpatialIndex index1, SpatialIndex index2){
        super.initializeJoinQuery(conf, index1, index2);
    }

    public DataStream<Tuple2<Point, LineString>> run(DataStream<Point> ordinaryPointStream, DataStream<LineString> queryStream, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex1();
        UniformGrid qGrid = (UniformGrid) this.getSpatialIndex2();

        //--------------- Real-time - POINT - LINESTRING -----------------//
        if(this.getQueryConfiguration().getQueryType() == QueryType.RealTime)
        {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTime(ordinaryPointStream, queryStream, queryRadius, uGrid, qGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - POINT - LINESTRING -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased)
        {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(ordinaryPointStream, queryStream, queryRadius, uGrid, qGrid, windowSize, slideStep, allowedLateness, approximateQuery);
        }

        else{
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple2<Point, LineString>> realTime(DataStream<Point> ordinaryPointStream, DataStream<LineString> queryStream, double queryRadius, UniformGrid uGrid, UniformGrid qGrid, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> replicatedQueryStream = JoinQuery.getReplicatedLineStringQueryStream(queryStream, queryRadius, qGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<LineString> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, LineString>> joinOutput = pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Point, LineString, Tuple2<Point,LineString>>() {
                    @Override
                    public Tuple2<Point, LineString> join(Point p, LineString q) {

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

        return joinOutput.filter(new FilterFunction<Tuple2<Point, LineString>>() {
            @Override
            public boolean filter(Tuple2<Point, LineString> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    // WINDOW BASED
    private DataStream<Tuple2<Point, LineString>> windowBased(DataStream<Point> ordinaryPointStream, DataStream<LineString> queryStream, double queryRadius, UniformGrid uGrid, UniformGrid qGrid, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> replicatedQueryStream = JoinQuery.getReplicatedLineStringQueryStream(queryStream, queryRadius, qGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<LineString> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, LineString>> joinOutput = pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Point, LineString, Tuple2<Point,LineString>>() {
                    @Override
                    public Tuple2<Point, LineString> join(Point p, LineString q) {

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

        return joinOutput.filter(new FilterFunction<Tuple2<Point, LineString>>() {
            @Override
            public boolean filter(Tuple2<Point, LineString> value) throws Exception {
                return value.f1 != null;
            }
        });
    }
}
