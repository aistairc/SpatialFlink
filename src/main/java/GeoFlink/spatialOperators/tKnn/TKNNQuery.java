package GeoFlink.spatialOperators.tKnn;

import GeoFlink.spatialIndices.SpatialIndex;

import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

public abstract class TKNNQuery<T extends SpatialObject, S extends SpatialObject, K extends SpatialObject> implements Serializable {
    private QueryConfiguration queryConfiguration;
    private SpatialIndex spatialIndex;

    public QueryConfiguration getQueryConfiguration() {
        return queryConfiguration;
    }

    public void setQueryConfiguration(QueryConfiguration queryConfiguration) {
        this.queryConfiguration = queryConfiguration;
    }

    public SpatialIndex getSpatialIndex() {
        return spatialIndex;
    }

    public void setSpatialIndex(SpatialIndex spatialIndex) {
        this.spatialIndex = spatialIndex;
    }

    public void initializeTKNNQuery(QueryConfiguration conf, SpatialIndex index){
        this.setQueryConfiguration(conf);
        this.setSpatialIndex(index);
    }

    public abstract DataStream<Tuple2<T, Double>> run(DataStream<S> ordinaryStream, K obj, double queryRadius, Integer k);

    // Returns Tuple2<String, Double>
    protected class kNNEvaluationWindowed implements WindowFunction<Point, Tuple2<String, Double>, String, TimeWindow> {

        //ctor
        public kNNEvaluationWindowed(){}

        public kNNEvaluationWindowed(Point qPoint, Integer k){
            this.queryPoint = qPoint;
            this.k = k;
        }

        Point queryPoint;
        Integer k;
        Map<String, Double> objMap = new HashMap<String, Double>();
        HashMap<String, Double> sortedObjMap = new LinkedHashMap<>();

        @Override
        public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<Tuple2<String, Double>> outputStream) throws Exception {

            objMap.clear();
            sortedObjMap.clear();

            // compute the distance of all trajectory points w.r.t. query point and return the kNN (trajectory ID, distance) pairs
            for (Point p : inputTuples) {

                Double newDistance = DistanceFunctions.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
                Double existingDistance = objMap.get(p.objID);

                if (existingDistance == null) { // if object with the given ObjID does not already exist
                    objMap.put(p.objID, newDistance);
                } else { // object already exist
                    if (newDistance < existingDistance)
                        objMap.replace(p.objID, newDistance);
                }
            }

            // Sorting the map by value
            List<Map.Entry<String, Double>> list = new LinkedList<>(objMap.entrySet());
            Collections.sort(list, Comparator.comparing(o -> o.getValue()));

            for (Map.Entry<String, Double> map : list) {
                sortedObjMap.put(map.getKey(), map.getValue());
            }

            // Logic to return the kNN (trajectory ID, distance) pairs
            int counter = 0;
            for (Map.Entry<String, Double> entry : sortedObjMap.entrySet()) {
                if (counter == k) break;
                outputStream.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                counter++;
            }
        }
    }

    // Returns Tuple2<Point, Double>
    protected class kNNEvaluationRealtime implements WindowFunction<Point, Tuple2<Point, Double>, String, TimeWindow> {

        //ctor
        public kNNEvaluationRealtime(){}

        public kNNEvaluationRealtime(Point qPoint, Integer k){
            this.queryPoint = qPoint;
            this.k = k;
        }

        Point queryPoint;
        Integer k;
        Map<String, Double> objIDDistMap = new HashMap<String, Double>();
        Map<String, Point> objIDPointMap = new HashMap<String, Point>();
        HashMap<String, Double> sortedObjMap = new LinkedHashMap<>();

        @Override
        public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<Tuple2<Point, Double>> outputStream) throws Exception {

            objIDDistMap.clear();
            objIDPointMap.clear();
            sortedObjMap.clear();

            // compute the distance of all trajectory points w.r.t. query point and return the kNN (trajectory ID, distance) pairs
            for (Point p : inputTuples) {
                Double newDistance = DistanceFunctions.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
                Double existingDistance = objIDDistMap.get(p.objID);

                if (existingDistance == null) { // if object with the given ObjID does not already exist
                    objIDDistMap.put(p.objID, newDistance);
                    objIDPointMap.put(p.objID, p);
                } else { // object already exist
                    if (newDistance < existingDistance) {
                        objIDDistMap.replace(p.objID, newDistance);
                        objIDPointMap.replace(p.objID, p);
                    }
                }
            }

            // Sorting the map by value
            List<Map.Entry<String, Double>> list = new LinkedList<>(objIDDistMap.entrySet());
            Collections.sort(list, Comparator.comparing(o -> o.getValue()));

            for (Map.Entry<String, Double> map : list) {
                sortedObjMap.put(map.getKey(), map.getValue());
            }

            // Logic to return the kNN (trajectory ID, distance) pairs
            int counter = 0;
            for (Map.Entry<String, Double> entry : sortedObjMap.entrySet()) {
                if (counter == k) break;
                outputStream.collect(Tuple2.of(objIDPointMap.get(entry.getKey()), entry.getValue()));
                counter++;
            }
        }
    }
}
