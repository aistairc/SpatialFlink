package GeoFlink.spatialOperators.join;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
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

public class LineStringPointJoinQuery extends JoinQuery<LineString, Point> {
    public LineStringPointJoinQuery(QueryConfiguration conf, SpatialIndex index1, SpatialIndex index2){
        super.initializeJoinQuery(conf, index1, index2);
    }

    public DataStream<Tuple2<LineString, Point>> run(DataStream<LineString> lineStringStream, DataStream<Point> queryPointStream, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex1();
        UniformGrid qGrid = (UniformGrid) this.getSpatialIndex2();

        //--------------- Real-time - LINESTRING - POINT -----------------//
        if(this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTime(lineStringStream, queryPointStream, queryRadius, omegaJoinDurationSeconds, uGrid, qGrid, allowedLateness, approximateQuery);
        }

        //--------------- Window-based - LINESTRING - POINT -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(lineStringStream, queryPointStream, queryRadius, windowSize, slideStep, uGrid, qGrid, allowedLateness, approximateQuery);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple2<LineString, Point>> realTime(DataStream<LineString> lineStringStream, DataStream<Point> queryPointStream, double queryRadius, int omegaJoinDurationSeconds, UniformGrid uGrid, UniformGrid qGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> queryPointStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> lineStringStreamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString ls) {
                        return ls.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedPointQueryStream(queryPointStreamWithTsAndWm, queryRadius, qGrid);
        DataStream<LineString> replicatedLineStringStream = lineStringStreamWithTsAndWm.flatMap(new HelperClass.ReplicateLineStringStreamUsingObjID());

        DataStream<Tuple2<LineString, Point>> joinOutput = replicatedLineStringStream.join(replicatedQueryStream)
                .where(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString ls) throws Exception {
                        return ls.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new JoinFunction<LineString, Point, Tuple2<LineString, Point>>() {
                    @Override
                    public Tuple2<LineString, Point> join(LineString ls, Point q) {

                        double distance;
                        if(approximateQuery) {
                            distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(q, ls);
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

        return joinOutput.filter(new FilterFunction<Tuple2<LineString, Point>>() {
            @Override
            public boolean filter(Tuple2<LineString, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    // WINDOW BASED
    private DataStream<Tuple2<LineString, Point>> windowBased(DataStream<LineString> lineStringStream, DataStream<Point> queryPointStream, double queryRadius, int windowSize, int slideStep, UniformGrid uGrid, UniformGrid qGrid, int allowedLateness, boolean approximateQuery){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: allowedLateness
        DataStream<Point> queryPointStreamWithTsAndWm =
                queryPointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<LineString> lineStringStreamWithTsAndWm =
                lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(LineString ls) {
                        return ls.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedPointQueryStream(queryPointStreamWithTsAndWm, queryRadius, qGrid);
        DataStream<LineString> replicatedLineStringStream = lineStringStreamWithTsAndWm.flatMap(new HelperClass.ReplicateLineStringStreamUsingObjID());

        DataStream<Tuple2<LineString, Point>> joinOutput = replicatedLineStringStream.join(replicatedQueryStream)
                .where(new KeySelector<LineString, String>() {
                    @Override
                    public String getKey(LineString ls) throws Exception {
                        return ls.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<LineString, Point, Tuple2<LineString, Point>>() {
                    @Override
                    public Tuple2<LineString, Point> join(LineString ls, Point q) {

                        double distance;
                        if(approximateQuery) {
                            distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(q, ls);
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

        return joinOutput.filter(new FilterFunction<Tuple2<LineString, Point>>() {
            @Override
            public boolean filter(Tuple2<LineString, Point> value) throws Exception {
                return value.f1 != null;
            }
        });
    }
}
