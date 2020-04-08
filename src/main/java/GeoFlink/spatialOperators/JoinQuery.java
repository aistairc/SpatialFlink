package GeoFlink.spatialOperators;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.io.Serializable;
import java.util.HashSet;

public class JoinQuery implements Serializable {

    public static DataStream<Tuple2<String, String>> SpatialJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double queryRadius, int windowSize, int slideStep, UniformGrid uGrid){

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedQueryStream(queryPointStream, queryRadius, uGrid);

        DataStream<Tuple2<String, String>> joinOutput = ordinaryPointStream.join(replicatedQueryStream)
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
                .apply(new JoinFunction<Point, Point, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> join(Point p, Point q) {
                        if (HelperClass.computeEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()) <= queryRadius) {
                            return Tuple2.of(p.gridID, q.gridID);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    public static DataStream<Point> getReplicatedQueryStream(DataStream<Point> queryPoints, double queryRadius, UniformGrid uGrid){

        return queryPoints.flatMap(new FlatMapFunction<Point, Point>() {
            @Override
            public void flatMap(Point queryPoint, Collector<Point> out) throws Exception {

                // Neighboring cells contain all the cells including Candidate cells, Guaranteed Cells and the query point cell itself
                HashSet<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);

                // Create duplicated query points
                for (String cellID: neighboringCells) {
                    Point p = new Point(queryPoint.objID, queryPoint.point.getX(), queryPoint.point.getY(), cellID);
                    out.collect(p);
                }
            }
        });
    }
}
