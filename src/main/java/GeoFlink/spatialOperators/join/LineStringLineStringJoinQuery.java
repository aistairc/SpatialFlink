package GeoFlink.spatialOperators.join;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.DistanceFunctions;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class LineStringLineStringJoinQuery extends JoinQuery<LineString, LineString> {
    public LineStringLineStringJoinQuery(QueryConfiguration conf, SpatialIndex index1, SpatialIndex index2){
        super.initializeJoinQuery(conf, index1, index2);
    }

    public DataStream<Tuple2<LineString, LineString>> run(DataStream<LineString> lineStringStream, DataStream<LineString> queryStream, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex1();
        UniformGrid qGrid = (UniformGrid) this.getSpatialIndex2();

        //--------------- Real-time - LINESTRING - LINESTRING -----------------//
        if(this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTime(lineStringStream, queryStream, queryRadius, uGrid, qGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - LINESTRING - LINESTRING -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(lineStringStream, queryStream, queryRadius, uGrid, qGrid, windowSize, slideStep, allowedLateness, approximateQuery);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple2<LineString, LineString>> realTime(DataStream<LineString> lineStringStream, DataStream<LineString> queryStream, double queryRadius, UniformGrid uGrid, UniformGrid qGrid, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<LineString> queryStreamWithTsAndWm =
                queryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> ordinaryStreamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> replicatedQueryStream = JoinQuery.getReplicatedLineStringQueryStream(queryStreamWithTsAndWm, queryRadius, qGrid);
        DataStream<LineString> replicatedLineStringStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicateLineStringStreamUsingObjID());

        DataStream<Tuple2<LineString, LineString>> joinOutput = replicatedLineStringStream.join(replicatedQueryStream)
                .where(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString ls) throws Exception {
                        return ls.gridID;
                    }
                }).equalTo(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<LineString, LineString, Tuple2<LineString, LineString>>() {
                    @Override
                    public Tuple2<LineString, LineString> join(LineString ls, LineString q) {

                        double distance;
                        if(approximateQuery) {
                            distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(q.boundingBox, ls.boundingBox);
                        }else{
                            distance = DistanceFunctions.getDistance(q, ls);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(ls, q);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<LineString, LineString>>() {
            @Override
            public boolean filter(Tuple2<LineString, LineString> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    // WINDOW BASED
    private DataStream<Tuple2<LineString, LineString>> windowBased(DataStream<LineString> lineStringStream, DataStream<LineString> queryStream, double queryRadius, UniformGrid uGrid, UniformGrid qGrid, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<LineString> queryStreamWithTsAndWm =
                queryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> ordinaryStreamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> replicatedQueryStream = JoinQuery.getReplicatedLineStringQueryStream(queryStreamWithTsAndWm, queryRadius, qGrid);
        DataStream<LineString> replicatedLineStringStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicateLineStringStreamUsingObjID());

        DataStream<Tuple2<LineString, LineString>> joinOutput = replicatedLineStringStream.join(replicatedQueryStream)
                .where(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString ls) throws Exception {
                        return ls.gridID;
                    }
                }).equalTo(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<LineString, LineString, Tuple2<LineString, LineString>>() {
                    @Override
                    public Tuple2<LineString, LineString> join(LineString ls, LineString q) {

                        double distance;
                        if(approximateQuery) {
                            distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(q.boundingBox, ls.boundingBox);
                        }else{
                            distance = DistanceFunctions.getDistance(q, ls);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(ls, q);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<LineString, LineString>>() {
            @Override
            public boolean filter(Tuple2<LineString, LineString> value) throws Exception {
                return value.f1 != null;
            }
        });
    }
}
