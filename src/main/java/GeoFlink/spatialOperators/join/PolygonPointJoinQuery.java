package GeoFlink.spatialOperators.join;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
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

public class PolygonPointJoinQuery extends JoinQuery<Polygon, Point> {
    public PolygonPointJoinQuery(QueryConfiguration conf, SpatialIndex index1, SpatialIndex index2){
        super.initializeJoinQuery(conf, index1, index2);
    }

    public DataStream<Tuple2<Polygon, Point>> run(DataStream<Polygon> polygonStream, DataStream<Point> queryPointStream, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex1();
        UniformGrid qGrid = (UniformGrid) this.getSpatialIndex2();

        //--------------- Real-time - POLYGON - POINT -----------------//
        if(this.getQueryConfiguration().getQueryType() == QueryType.RealTime)
        {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTime(polygonStream, queryPointStream, queryRadius, omegaJoinDurationSeconds, uGrid, qGrid, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - POLYGON - POINT -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased)
        {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(polygonStream, queryPointStream, queryRadius, windowSize, slideStep, uGrid, qGrid, allowedLateness, approximateQuery);
        }

        else{
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple2<Polygon, Point>> realTime(DataStream<Polygon> polygonStream, DataStream<Point> queryPointStream, double queryRadius, int omegaJoinDurationSeconds, UniformGrid uGrid, UniformGrid qGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> queryPointStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> polygonStreamWithTsAndWm =
                polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedPointQueryStream(queryPointStreamWithTsAndWm, queryRadius, qGrid);
        DataStream<Polygon> replicatedPolygonStream = polygonStreamWithTsAndWm.flatMap(new HelperClass.ReplicatePolygonStreamUsingObjID());

        DataStream<Tuple2<Polygon, Point>> joinOutput = replicatedPolygonStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Polygon, Point, Tuple2<Polygon, Point>>() {
                    @Override
                    public Tuple2<Polygon, Point> join(Polygon poly, Point q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(q, poly);
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

        return joinOutput.filter(new FilterFunction<Tuple2<Polygon, Point>>() {
            @Override
            public boolean filter(Tuple2<Polygon, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    // WINDOW BASED
    private DataStream<Tuple2<Polygon, Point>> windowBased(DataStream<Polygon> polygonStream, DataStream<Point> queryPointStream, double queryRadius, int windowSize, int slideStep, UniformGrid uGrid, UniformGrid qGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> queryPointStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> polygonStreamWithTsAndWm =
                polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedPointQueryStream(queryPointStreamWithTsAndWm, queryRadius, qGrid);
        DataStream<Polygon> replicatedPolygonStream = polygonStreamWithTsAndWm.flatMap(new HelperClass.ReplicatePolygonStreamUsingObjID());

        DataStream<Tuple2<Polygon, Point>> joinOutput = replicatedPolygonStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Polygon, Point, Tuple2<Polygon, Point>>() {
                    @Override
                    public Tuple2<Polygon, Point> join(Polygon poly, Point q) {

                        Double distance;
                        if(approximateQuery) {
                            distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(q, poly);
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

        return joinOutput.filter(new FilterFunction<Tuple2<Polygon, Point>>() {
            @Override
            public boolean filter(Tuple2<Polygon, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }
}
