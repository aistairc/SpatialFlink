package GeoFlink.spatialOperators.tKnn;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class PointPointPointTKNNQuery extends TKNNQuery<Point, Point, Point> {
    public PointPointPointTKNNQuery(QueryConfiguration conf, SpatialIndex index) {
        super.initializeTKNNQuery(conf, index);
    }

    public DataStream<Tuple2<Point, Double>> run(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k) {
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

        //--------------- Real-time - LINESTRING - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realtime(pointStream, queryPoint, queryRadius, k, omegaJoinDurationSeconds, allowedLateness, uGrid);
        }

        //--------------- Window-based - LINESTRING - POLYGON -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            throw new IllegalArgumentException("Not yet support");
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple2<Point, Double>> realtime(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k, int omegaJoinDurationSeconds, int allowedLateness, UniformGrid uGrid) {

        Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                return (neighboringCells.contains(point.gridID));
            }
        });

        //filteredPoints.print();

        // Output at-most k objIDs and their distances from point p
        DataStream<Tuple2<Point, Double>> kNNStream = filteredPoints.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.gridID;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new TKNNQuery.kNNEvaluationRealtime(queryPoint, k));


        //kNNStream.print();

        // Logic to integrate all the kNNs to produce an integrated kNN
        return kNNStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(omegaJoinDurationSeconds)))
                .apply(new AllWindowFunction<Tuple2<Point, Double>, Tuple2<Point, Double>, TimeWindow>() {

                    //Map of objID and Point
                    Map<String, Point> pointsIDMap = new HashMap<>();
                    //Map of objID and distFromQueryPoint
                    Map<String, Double> pointDistFromQueryPoint = new HashMap<>();
                    //Map for sorting distFromQueryPoint
                    HashMap<String, Double> sortedPointDistFromQueryPoint = new LinkedHashMap<>();

                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<Point, Double>> input, Collector<Tuple2<Point, Double>> output) throws Exception {

                        pointsIDMap.clear();
                        pointDistFromQueryPoint.clear();
                        sortedPointDistFromQueryPoint.clear();

                        // Collect all the points in two maps
                        for (Tuple2<Point, Double> e : input) {

                            Double existingDistance = pointDistFromQueryPoint.get(e.f0.objID);
                            // Check if a point already exist, if not insert it, else replace previous point if the current point distance is less than previous point with the same ID
                            if (existingDistance == null) { // if object with the given ObjID does not already exist
                                pointDistFromQueryPoint.put(e.f0.objID, e.f1);
                                pointsIDMap.put(e.f0.objID, e.f0);
                            } else { // object already exist
                                if (e.f1 < existingDistance) {
                                    pointDistFromQueryPoint.replace(e.f0.objID, e.f1);
                                    pointsIDMap.replace(e.f0.objID, e.f0);
                                }
                            }
                        }

                        // Sorting the pointDistFromQueryPoint map by value
                        List<Map.Entry<String, Double>> list = new LinkedList<>(pointDistFromQueryPoint.entrySet());
                        Collections.sort(list, Comparator.comparing(o -> o.getValue()));
                        for (Map.Entry<String, Double> map : list) {
                            sortedPointDistFromQueryPoint.put(map.getKey(), map.getValue());
                        }

                        // Logic to return the kNN (trajectory ID, distance) pairs
                        int counter = 0;
                        for (Map.Entry<String, Double> entry: sortedPointDistFromQueryPoint.entrySet()) {
                            if (counter == k) break; // to guarantee that only k outputs are generated
                            output.collect(Tuple2.of(pointsIDMap.get(entry.getKey()), entry.getValue()));
                            counter++;
                        }
                    }
                });
    }
}
