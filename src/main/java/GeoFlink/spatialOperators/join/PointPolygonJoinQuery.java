package GeoFlink.spatialOperators.join;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
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

import java.util.Date;

public class PointPolygonJoinQuery extends JoinQuery<Point, Polygon> {
    public PointPolygonJoinQuery(QueryConfiguration conf, SpatialIndex index1, SpatialIndex index2){
        super.initializeJoinQuery(conf, index1, index2);
    }

    public DataStream<Tuple2<Point, Polygon>> run(DataStream<Point> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex1();
        UniformGrid qGrid = (UniformGrid) this.getSpatialIndex2();

        //--------------- Real-time - POINT - POLYGON -----------------//
        if(this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTime(ordinaryPointStream, queryPolygonStream, uGrid, qGrid, queryRadius, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - POINT - POLYGON -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(ordinaryPointStream, queryPolygonStream, uGrid, qGrid, queryRadius, windowSize, slideStep, allowedLateness, approximateQuery);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    public DataStream<Long> runLatency(DataStream<Point> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex1();
        UniformGrid qGrid = (UniformGrid) this.getSpatialIndex2();

        //--------------- Real-time - POINT - POLYGON -----------------//
        if(this.getQueryConfiguration().getQueryType() == QueryType.RealTime)
        {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return commonLatency(ordinaryPointStream, queryPolygonStream, uGrid, qGrid, queryRadius, omegaJoinDurationSeconds, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - POINT - POLYGON -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased)
        {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();
            return commonLatency(ordinaryPointStream, queryPolygonStream, uGrid, qGrid, queryRadius, windowSize, slideStep, allowedLateness, approximateQuery);
        }

        else{
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple2<Point, Polygon>> realTime(DataStream<Point> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, UniformGrid qGrid, double queryRadius, int omegaJoinDurationSeconds, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryPolygonStream, queryRadius, qGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, Polygon>> joinOutput = pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<Point, Polygon, Tuple2<Point,Polygon>>() {
                    @Override
                    public Tuple2<Point, Polygon> join(Point p, Polygon q) {

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

        return joinOutput.filter(new FilterFunction<Tuple2<Point, Polygon>>() {
            @Override
            public boolean filter(Tuple2<Point, Polygon> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    // WINDOW BASED
    private DataStream<Tuple2<Point, Polygon>> windowBased(DataStream<Point> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, UniformGrid qGrid, double queryRadius, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryPolygonStream, queryRadius, qGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Tuple2<Point, Polygon>> joinOutput = pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Point, Polygon, Tuple2<Point,Polygon>>() {
                    @Override
                    public Tuple2<Point, Polygon> join(Point p, Polygon q) {

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

        return joinOutput.filter(new FilterFunction<Tuple2<Point, Polygon>>() {
            @Override
            public boolean filter(Tuple2<Point, Polygon> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    // REAL-TIME / WINDOW BASED
    private DataStream<Long> commonLatency(DataStream<Point> ordinaryPointStream, DataStream<Polygon> queryPolygonStream, UniformGrid uGrid, UniformGrid qGrid, double queryRadius, int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> pointStreamWithTsAndWm =
                ordinaryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedPolygonQueryStream(queryPolygonStream, queryRadius, qGrid);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Polygon> replicatedQueryStreamWithTsAndWm =
                replicatedQueryStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        return pointStreamWithTsAndWm.join(replicatedQueryStreamWithTsAndWm)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Point, Polygon, Long>() {
                    @Override
                    public Long join(Point p, Polygon q) {

                        if (approximateQuery) { // all the candidate neighbors are sent to output
                            Date date = new Date();
                            return date.getTime() -  p.timeStampMillisec;
                        } else {

                            if (DistanceFunctions.getDistance(p, q) <= queryRadius) {
                                Date date = new Date();
                                return date.getTime() -  p.timeStampMillisec;
                            }
                            else{
                                Date date = new Date();
                                return date.getTime() -  p.timeStampMillisec;
                            }
                        }
                    }
                });
    }
}
