package GeoFlink.spatialOperators.join;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Polygon;
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

public class PolygonPolygonJoinQuery extends JoinQuery<Polygon, Polygon> {
    public PolygonPolygonJoinQuery(QueryConfiguration conf, SpatialIndex index1, SpatialIndex index2){
        super.initializeJoinQuery(conf, index1, index2);
    }

    public DataStream<Tuple2<Polygon, Polygon>> run(DataStream<Polygon> polygonStream, DataStream<Polygon> queryPolygonStream, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex1();
        UniformGrid qGrid = (UniformGrid) this.getSpatialIndex2();

        //--------------- Real-time - POLYGON - POLYGON -----------------//
        if(this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTime(polygonStream, queryPolygonStream, uGrid, qGrid, queryRadius, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - POLYGON - POLYGON -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(polygonStream, queryPolygonStream, uGrid, qGrid, queryRadius, windowSize, slideStep, allowedLateness, approximateQuery);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple2<Polygon, Polygon>> realTime(DataStream<Polygon> polygonStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, UniformGrid qGrid, double queryRadius, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> queryStreamWithTsAndWm =
                queryPolygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> ordinaryStreamWithTsAndWm =
                polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryStreamWithTsAndWm, queryRadius, qGrid);
        DataStream<Polygon> replicatedOrdinaryStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicatePolygonStreamUsingObjID());

        DataStream<Tuple2<Polygon, Polygon>> joinOutput = replicatedOrdinaryStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Polygon, Polygon, Tuple2<Polygon, Polygon>>() {
                    @Override
                    public Tuple2<Polygon, Polygon> join(Polygon poly, Polygon q) {

                        double distance;
                        if(approximateQuery) {
                            distance = DistanceFunctions.getPolygonPolygonBBoxMinEuclideanDistance(q, poly);
                        }else{
                            distance = DistanceFunctions.getDistance(q, poly);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(poly, q);
                        } else {
                            return Tuple2.of(null, null);
                        }

                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Polygon, Polygon>>() {
            @Override
            public boolean filter(Tuple2<Polygon, Polygon> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    // WINDOW BASED
    private DataStream<Tuple2<Polygon, Polygon>> windowBased(DataStream<Polygon> polygonStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, UniformGrid qGrid, double queryRadius, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> queryStreamWithTsAndWm =
                queryPolygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> ordinaryStreamWithTsAndWm =
                polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryStreamWithTsAndWm, queryRadius, qGrid);
        DataStream<Polygon> replicatedOrdinaryStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicatePolygonStreamUsingObjID());

        DataStream<Tuple2<Polygon, Polygon>> joinOutput = replicatedOrdinaryStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Polygon, Polygon, Tuple2<Polygon, Polygon>>() {
                    @Override
                    public Tuple2<Polygon, Polygon> join(Polygon poly, Polygon q) {

                        double distance;
                        if(approximateQuery) {
                            distance = DistanceFunctions.getPolygonPolygonBBoxMinEuclideanDistance(q, poly);
                        }else{
                            distance = DistanceFunctions.getDistance(q, poly);
                        }

                        if (distance <= queryRadius) {
                            return Tuple2.of(poly, q);
                        } else {
                            return Tuple2.of(null, null);
                        }

                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<Polygon, Polygon>>() {
            @Override
            public boolean filter(Tuple2<Polygon, Polygon> value) throws Exception {
                return value.f1 != null;
            }
        });
    }
}
