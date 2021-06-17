package GeoFlink.spatialOperators.join;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
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

public class LineStringPolygonJoinQuery extends JoinQuery<LineString, Polygon> {
    public LineStringPolygonJoinQuery(QueryConfiguration conf, SpatialIndex index1, SpatialIndex index2){
        super.initializeJoinQuery(conf, index1, index2);
    }

    public DataStream<Tuple2<LineString, Polygon>> run(DataStream<LineString> lineStringStream, DataStream<Polygon> queryStream, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex1();
        UniformGrid qGrid = (UniformGrid) this.getSpatialIndex2();

        //--------------- Real-time - LINESTRING - POLYGON -----------------//
        if(this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTime(lineStringStream, queryStream, uGrid, qGrid, queryRadius, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - LINESTRING - POLYGON -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(lineStringStream, queryStream, uGrid, qGrid, queryRadius, windowSize, slideStep, allowedLateness, approximateQuery);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple2<LineString, Polygon>> realTime(DataStream<LineString> lineStringStream, DataStream<Polygon> queryStream, UniformGrid uGrid, UniformGrid qGrid, double queryRadius, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> queryStreamWithTsAndWm =
                queryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
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

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryStreamWithTsAndWm, queryRadius, qGrid);
        DataStream<LineString> replicatedLineStringStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicateLineStringStreamUsingObjID());

        DataStream<Tuple2<LineString, Polygon>> joinOutput = replicatedLineStringStream.join(replicatedQueryStream)
                .where(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString ls) throws Exception {
                        return ls.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<LineString, Polygon, Tuple2<LineString, Polygon>>() {
                    @Override
                    public Tuple2<LineString, Polygon> join(LineString ls, Polygon q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = DistanceFunctions.getPolygonLineStringBBoxMinEuclideanDistance(q, ls);
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

        return joinOutput.filter(new FilterFunction<Tuple2<LineString, Polygon>>() {
            @Override
            public boolean filter(Tuple2<LineString, Polygon> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    // WINDOW BASED
    private DataStream<Tuple2<LineString, Polygon>> windowBased(DataStream<LineString> lineStringStream, DataStream<Polygon> queryStream, UniformGrid uGrid, UniformGrid qGrid, double queryRadius, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> queryStreamWithTsAndWm =
                queryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
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

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryStreamWithTsAndWm, queryRadius, qGrid);
        DataStream<LineString> replicatedLineStringStream = ordinaryStreamWithTsAndWm.flatMap(new HelperClass.ReplicateLineStringStreamUsingObjID());

        DataStream<Tuple2<LineString, Polygon>> joinOutput = replicatedLineStringStream.join(replicatedQueryStream)
                .where(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString ls) throws Exception {
                        return ls.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<LineString, Polygon, Tuple2<LineString, Polygon>>() {
                    @Override
                    public Tuple2<LineString, Polygon> join(LineString ls, Polygon q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = DistanceFunctions.getPolygonLineStringBBoxMinEuclideanDistance(q, ls);
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

        return joinOutput.filter(new FilterFunction<Tuple2<LineString, Polygon>>() {
            @Override
            public boolean filter(Tuple2<LineString, Polygon> value) throws Exception {
                return value.f1 != null;
            }
        });
    }
}
