package GeoFlink.spatialOperators.knn;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.SpatialOperator;
import GeoFlink.utils.Comparators;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

public abstract class KNNQuery<T extends SpatialObject, K extends SpatialObject> extends SpatialOperator implements Serializable {
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

    public void initializeKNNQuery(QueryConfiguration conf, SpatialIndex index){
        this.setQueryConfiguration(conf);
        this.setSpatialIndex(index);
    }

    public abstract DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<T, Double>>>> run(DataStream<T> ordinaryStream, K obj, double queryRadius, Integer k) throws IOException;

    // Returns Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>
    protected class kNNWinAllEvaluationLineStringStream implements AllWindowFunction<PriorityQueue<Tuple2<LineString, Double>>, Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>, TimeWindow> {

        //ctor
        public kNNWinAllEvaluationLineStringStream() {
        }

        public kNNWinAllEvaluationLineStringStream(Integer k) {
            this.k = k;
        }

        Integer k;

        @Override
        public void apply(TimeWindow timeWindow, Iterable<PriorityQueue<Tuple2<LineString, Double>>> input, Collector<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> output) throws Exception {

            PriorityQueue<Tuple2<LineString, Double>> kNNPQWinAll = new PriorityQueue<Tuple2<LineString, Double>>(k, new Comparators.inTupleLineStringDistanceComparator());
            Set<String> objIDs = new HashSet<String>();

            kNNPQWinAll.clear();
            // Iterate through all PriorityQueues
            for (PriorityQueue<Tuple2<LineString, Double>> pq : input) {
                for(Tuple2<LineString, Double> candidPQTuple: pq) {
                    // If there are less than required (k) number of tuples in kNNPQWinAll
                    if (kNNPQWinAll.size() < k) {
                        // Add an object if it is not already there
                        if(!objIDs.contains(candidPQTuple.f0.objID)) {
                            kNNPQWinAll.offer(candidPQTuple);
                            objIDs.add(candidPQTuple.f0.objID);
                        }else{
                            // (To avoid duplicate addition of an object in kNN) Object is already in PQ, check the existing object's distance compared to current object
                            for (Tuple2<LineString, Double> existingPQTuple : kNNPQWinAll){
                                if(existingPQTuple.f0.objID.equals(candidPQTuple.f0.objID) && existingPQTuple.f1 > candidPQTuple.f1){
                                    kNNPQWinAll.remove(existingPQTuple);
                                    kNNPQWinAll.offer(candidPQTuple);
                                    break;
                                }
                            }
                        }
                    }
                    // If there are already required (k) number of tuples in kNNPQWinAll
                    else{
                        assert kNNPQWinAll.peek() != null;
                        double largestDistInkNNPQ = kNNPQWinAll.peek().f1;
                        if (largestDistInkNNPQ > candidPQTuple.f1) {
                            // Add an object if it is not already there
                            if(!objIDs.contains(candidPQTuple.f0.objID)) {
                                // remove element with the largest distance and add the new element
                                kNNPQWinAll.poll();
                                assert kNNPQWinAll.peek() != null;
                                objIDs.remove(kNNPQWinAll.peek().f0.objID);

                                kNNPQWinAll.offer(candidPQTuple);
                                objIDs.add(candidPQTuple.f0.objID);
                            }
                            else {
                                // (To avoid duplicate addition of an object in kNN) Object is already in PQ, check the existing object's distance compared to current object
                                for (Tuple2<LineString, Double> existingPQTuple : kNNPQWinAll) {
                                    if (existingPQTuple.f0.objID.equals(candidPQTuple.f0.objID) && existingPQTuple.f1 > candidPQTuple.f1) {
                                        kNNPQWinAll.remove(existingPQTuple);
                                        kNNPQWinAll.offer(candidPQTuple);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Adding the windowedAll output
            output.collect(Tuple3.of(timeWindow.getStart(), timeWindow.getEnd(), kNNPQWinAll));

        }
    }


    // Returns Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>
    protected class kNNWinAllEvaluationPolygonStream implements AllWindowFunction  <PriorityQueue<Tuple2<Polygon, Double>>, Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>, TimeWindow> {

        Integer k;

        //ctor
        public kNNWinAllEvaluationPolygonStream() {
        }

        public kNNWinAllEvaluationPolygonStream(Integer k) {
            this.k = k;
        }

        @Override
        public void apply(TimeWindow timeWindow, Iterable<PriorityQueue<Tuple2<Polygon, Double>>> input, Collector<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> output) throws Exception {

            PriorityQueue<Tuple2<Polygon, Double>> kNNPQWinAll = new PriorityQueue<Tuple2<Polygon, Double>>(k, new Comparators.inTuplePolygonDistanceComparator());
            Set<String> objIDs = new HashSet<String>();

            kNNPQWinAll.clear();
            // Iterate through all PriorityQueues
            for (PriorityQueue<Tuple2<Polygon, Double>> pq : input) {
                for(Tuple2<Polygon, Double> candidPQTuple: pq) {
                    // If there are less than required (k) number of tuples in kNNPQWinAll
                    if (kNNPQWinAll.size() < k) {
                        // Add an object if it is not already there
                        if(!objIDs.contains(candidPQTuple.f0.objID)) {
                            kNNPQWinAll.offer(candidPQTuple);
                            objIDs.add(candidPQTuple.f0.objID);
                        }else{
                            // (To avoid duplicate addition of an object in kNN) Object is already in PQ, check the existing object's distance compared to current object
                            for (Tuple2<Polygon, Double> existingPQTuple : kNNPQWinAll){
                                if(existingPQTuple.f0.objID.equals(candidPQTuple.f0.objID) && existingPQTuple.f1 > candidPQTuple.f1){
                                    kNNPQWinAll.remove(existingPQTuple);
                                    kNNPQWinAll.offer(candidPQTuple);
                                    break;
                                }
                            }
                        }
                    }
                    // If there are already required (k) number of tuples in kNNPQWinAll
                    else{
                        assert kNNPQWinAll.peek() != null;
                        double largestDistInkNNPQ = kNNPQWinAll.peek().f1;
                        if (largestDistInkNNPQ > candidPQTuple.f1) {
                            // Add an object if it is not already there
                            if(!objIDs.contains(candidPQTuple.f0.objID)) {
                                // remove element with the largest distance and add the new element
                                kNNPQWinAll.poll();
                                assert kNNPQWinAll.peek() != null;
                                objIDs.remove(kNNPQWinAll.peek().f0.objID);

                                kNNPQWinAll.offer(candidPQTuple);
                                objIDs.add(candidPQTuple.f0.objID);
                            }
                            else {
                                // (To avoid duplicate addition of an object in kNN) Object is already in PQ, check the existing object's distance compared to current object
                                for (Tuple2<Polygon, Double> existingPQTuple : kNNPQWinAll) {
                                    if (existingPQTuple.f0.objID.equals(candidPQTuple.f0.objID) && existingPQTuple.f1 > candidPQTuple.f1) {
                                        kNNPQWinAll.remove(existingPQTuple);
                                        kNNPQWinAll.offer(candidPQTuple);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Adding the windowedAll output
            output.collect(Tuple3.of(timeWindow.getStart(), timeWindow.getEnd(), kNNPQWinAll));
        }
    }

    // Returns Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>
    protected class kNNWinAllEvaluationPointStream implements AllWindowFunction  <PriorityQueue<Tuple2<Point, Double>>, Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>, TimeWindow> {

        //ctor
        public kNNWinAllEvaluationPointStream(){}
        public kNNWinAllEvaluationPointStream(Integer k){
            this.k = k;
        }
        Integer k;

        @Override
        public void apply(TimeWindow timeWindow, Iterable<PriorityQueue<Tuple2<Point, Double>>> input, Collector<Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> output) throws Exception {

            PriorityQueue<Tuple2<Point, Double>> kNNPQWinAll = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());
            Set<String> objIDs = new HashSet<String>();

            kNNPQWinAll.clear();
            // Iterate through all PriorityQueues
            for (PriorityQueue<Tuple2<Point, Double>> pq : input) {
                for(Tuple2<Point, Double> candidPQTuple: pq) {
                    // If there are less than required (k) number of tuples in kNNPQWinAll
                    if (kNNPQWinAll.size() < k) {
                        // Add an object if it is not already there
                        if(!objIDs.contains(candidPQTuple.f0.objID)) {
                            kNNPQWinAll.add(candidPQTuple);
                            objIDs.add(candidPQTuple.f0.objID);
                        }else{
                            // If the object already exist (To avoid duplicate addition of an object in kNN) Object is already in PQ, check the existing object's distance compared to current object
                            for (Tuple2<Point, Double> existingPQTuple : kNNPQWinAll){
                                if(existingPQTuple.f0.objID.equals(candidPQTuple.f0.objID) && existingPQTuple.f1 > candidPQTuple.f1){
                                    kNNPQWinAll.remove(existingPQTuple);
                                    kNNPQWinAll.add(candidPQTuple);
                                    break;
                                }
                            }
                        }
                    }
                    // If there are already required (k) number of tuples in kNNPQWinAll
                    else{
                        assert kNNPQWinAll.peek() != null;
                        double largestDistInkNNPQ = kNNPQWinAll.peek().f1; // get the largest distance

                        if (largestDistInkNNPQ > candidPQTuple.f1) {
                            // Add an object if it is not already there
                            if(!objIDs.contains(candidPQTuple.f0.objID)) {
                                // remove element with the largest distance and add the new element
                                kNNPQWinAll.poll();
                                assert kNNPQWinAll.peek() != null;
                                objIDs.remove(kNNPQWinAll.peek().f0.objID);

                                kNNPQWinAll.offer(candidPQTuple);
                                objIDs.add(candidPQTuple.f0.objID);
                            }
                            else {
                                // If the object already exist (To avoid duplicate addition of an object in kNN) Object is already in PQ, check the existing object's distance compared to current object
                                for (Tuple2<Point, Double> existingPQTuple : kNNPQWinAll) {
                                    if (existingPQTuple.f0.objID.equals(candidPQTuple.f0.objID) && existingPQTuple.f1 > candidPQTuple.f1) {
                                        kNNPQWinAll.remove(existingPQTuple);
                                        kNNPQWinAll.offer(candidPQTuple);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Adding the windowedAll output
            output.collect(Tuple3.of(timeWindow.getStart(), timeWindow.getEnd(), kNNPQWinAll));

            /*
        // windowAll to Generate integrated kNN -
        DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> windowAllKNN = windowedKNN
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(windowSize),Time.seconds(windowSlideStep)))
                .apply(new AllWindowFunction<PriorityQueue<Tuple2<Point, Double>>, Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>, TimeWindow>() {

                    //PriorityQueue<Tuple2<Point, Double>> kNNPQWinAll = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.SpatialDistanceComparator(queryPoint));
                    PriorityQueue<Tuple2<Point, Double>> kNNPQWinAll = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());

                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<PriorityQueue<Tuple2<Point, Double>>> input, Collector<Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> output) throws Exception {
                        kNNPQWinAll.clear();
                        // Iterate through all PriorityQueues
                        for (PriorityQueue<Tuple2<Point, Double>> pq : input) {
                            for(Tuple2<Point, Double> pqTuple: pq) {
                                if (kNNPQWinAll.size() < k) {
                                    kNNPQWinAll.offer(pqTuple);
                                }
                                else{
                                    double largestDistInkNNPQ = kNNPQWinAll.peek().f1;
                                    if(largestDistInkNNPQ > pqTuple.f1){ // remove element with the largest distance and add the new element
                                        kNNPQWinAll.poll();
                                        kNNPQWinAll.offer(pqTuple);
                                    }
                                }
                            }
                        }

                        // Adding the windowedAll output
                        output.collect(Tuple3.of(timeWindow.getStart(), timeWindow.getEnd(), kNNPQWinAll));
                    }
                });
         */

        }
    }
}
