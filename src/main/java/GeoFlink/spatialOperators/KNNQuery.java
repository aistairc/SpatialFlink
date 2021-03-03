/*
Copyright 2020 Data Platform Research Team, AIRC, AIST, Japan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package GeoFlink.spatialOperators;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.Comparators;
import GeoFlink.utils.DistanceFunctions;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class KNNQuery implements Serializable {

    public KNNQuery() {}

    //--------------- GRID-BASED kNN QUERY - POINT - Iterative Distributed -----------------//
    public static DataStream<PriorityQueue<Tuple2<Point, Double>>> SpatialIterativeKNNQuery(DataStream<Point> pointStream, Point queryPoint, Integer k, int windowSize, int windowSlideStep, UniformGrid uGrid) throws IOException {

        // Control tuple oID = -99999
        IterativeStream<Point> iterativeKeyedStream = pointStream.iterate();

        // Iteration Body
        DataStream<Point> filteredStream = iterativeKeyedStream.filter(new FilterFunction<Point>() {

            int filterationNeighboringLayers = 0;
            double queryRadiusMultFactor = 1.5;
            ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryPoint.gridID);

            @Override
            public boolean filter(Point p) throws Exception {

                // Recompute filterationCellsSet on the arrival of control tuple
                if(p.gridID.equals("9999999999")){
                    filterationNeighboringLayers = uGrid.getCandidateNeighboringLayers(p.point.getX() * queryRadiusMultFactor);
                    //System.out.println("Received feedback tuple");
                    return false;
                }

                // Filtering out the kNN out of range tuples
                if(filterationNeighboringLayers == 0){
                    return true;
                }
                else {
                    ArrayList<Integer> pointCellIndices = HelperClass.getIntCellIndices(p.gridID);
                    return (HelperClass.pointWithinQueryRange(pointCellIndices, queryCellIndices, filterationNeighboringLayers));
                }
            }
        });

        DataStream<PriorityQueue<Tuple2<Point, Double>>> windowedIterativeStream = filteredStream
                .keyBy("gridID")
                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize),Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Point, PriorityQueue<Tuple2<Point, Double>>, Tuple, TimeWindow>() {

                    //PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new SpatialDistanceComparator(queryPoint));
                    PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<PriorityQueue<Tuple2<Point, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Point p : inputTuples) {

                            if (kNNPQ.size() < k) {
                                double distance = HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
                                kNNPQ.offer(new Tuple2<Point, Double>(p, distance));
                            } else {
                                double distance = HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
                                double largestDistInPQ = HelperClass.getPointPointEuclideanDistance(kNNPQ.peek().f0.point.getX(), kNNPQ.peek().f0.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());

                                if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                    kNNPQ.poll();
                                    kNNPQ.offer(new Tuple2<Point, Double>(p, distance));
                                }
                            }
                        }

                        // Output stream
                        outputStream.collect(kNNPQ);
                    }
                });

        // windowAll to Generate integrated kNN -
        DataStream<PriorityQueue<Tuple2<Point, Double>>> windowAllIterativeStream = windowedIterativeStream
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize),Time.seconds(windowSlideStep)))
                .apply(new AllWindowFunction<PriorityQueue<Tuple2<Point, Double>>, PriorityQueue<Tuple2<Point, Double>>, TimeWindow>() {

                    //PriorityQueue<Tuple2<Point, Double>> kNNPQWinAll = new PriorityQueue<Tuple2<Point, Double>>(k, new SpatialDistanceComparator(queryPoint));
                    //PriorityQueue<Tuple2<Point, Double>> controlPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new SpatialDistanceComparator(queryPoint));

                    PriorityQueue<Tuple2<Point, Double>> kNNPQWinAll = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());
                    PriorityQueue<Tuple2<Point, Double>> controlPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());


                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<PriorityQueue<Tuple2<Point, Double>>> input, Collector<PriorityQueue<Tuple2<Point, Double>>> output) throws Exception {

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
                       output.collect(kNNPQWinAll);

                        // Adding the control tuple
                        double largestDistInkNNPQ = kNNPQWinAll.peek().f1;
                        //Point feedbackTuple = new Point(-99999, largestDistInkNNPQ, largestDistInkNNPQ, "9999999999" );
                        Point feedbackTuple = new Point(largestDistInkNNPQ, largestDistInkNNPQ, "9999999999" );
                        controlPQ.offer(new Tuple2<Point, Double>(feedbackTuple, -99999.99999));

                        output.collect(controlPQ);
                    }
                   });



        // Assuming that the distance between two objects is always >= 0
        // Feedback
        DataStream<Point> feedbackStream = windowAllIterativeStream.flatMap(new FlatMapFunction<PriorityQueue<Tuple2<Point, Double>>, Point>() {
            @Override
            public void flatMap(PriorityQueue<Tuple2<Point, Double>> inputStream, Collector<Point> outputStream) throws Exception {
                // If the control tuple exists
                if(inputStream.peek().f1 < 0){
                    outputStream.collect(inputStream.peek().f0);
                }
            }
        });

        iterativeKeyedStream.closeWith(feedbackStream);
        //Output
        return windowAllIterativeStream.filter(new FilterFunction<PriorityQueue<Tuple2<Point, Double>>>() {
            @Override
            public boolean filter(PriorityQueue<Tuple2<Point, Double>> inputTuple) throws Exception {
                return (inputTuple.peek().f1 >= 0);
            }
        });
    }

    //--------------- GRID-BASED kNN QUERY - POINT -----------------//
    public static DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> SpatialKNNQuery(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k, int windowSize, int windowSlideStep, UniformGrid uGrid) throws IOException {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint.gridID, guaranteedNeighboringCells);

        DataStream<Point> filteredPoints = pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
            }
        });

        DataStream<PriorityQueue<Tuple2<Point, Double>>> windowedKNN = filteredPoints.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Point, PriorityQueue<Tuple2<Point, Double>>, String, TimeWindow>() {

                    //PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new SpatialDistanceComparator(queryPoint));
                    PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new Comparators.inTuplePointDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<PriorityQueue<Tuple2<Point, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Point p : inputTuples) {

                            if (kNNPQ.size() < k) {
                                double distance = HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
                                kNNPQ.offer(new Tuple2<Point, Double>(p, distance));
                            } else {
                                double distance = HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
                                //double largestDistInPQ = HelperClass.getPointPointEuclideanDistance(kNNPQ.peek().f0.point.getX(), kNNPQ.peek().f0.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
                                // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                double largestDistInPQ = kNNPQ.peek().f1;

                                if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                    kNNPQ.poll();
                                    kNNPQ.offer(new Tuple2<Point, Double>(p, distance));
                                }
                            }
                        }

                        // Output stream
                        outputStream.collect(kNNPQ);
                    }
                }).name("Windowed (Apply) Grid Based");


        // windowAll to Generate integrated kNN -
        DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> windowAllKNN = windowedKNN
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize),Time.seconds(windowSlideStep)))
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

        //Output kNN Stream
        return windowAllKNN;
    }


    //--------------- GRID-BASED kNN QUERY - POINT-POLYGON -----------------//
    //Outputs a stream of winStartTime, winEndTime and a PQ
    public static DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> SpatialKNNQuery(DataStream<Polygon> polygonStream, Point queryPoint, double queryRadius, Integer k, UniformGrid uGrid, int windowSize, int windowSlideStep, boolean approximateQuery) throws IOException {

        Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);
        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);
        // Filtering out the polygons which lie greater than queryRadius of the query point
        DataStream<Polygon> filteredPolygons = polygonStream.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells));

        DataStream<PriorityQueue<Tuple2<Polygon, Double>>> windowedKNN = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
            @Override
            public String getKey(Polygon poly) throws Exception {
                return poly.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Polygon, PriorityQueue<Tuple2<Polygon, Double>>, String, TimeWindow>() {

                    PriorityQueue<Tuple2<Polygon, Double>> kNNPQ = new PriorityQueue<Tuple2<Polygon, Double>>(k, new Comparators.inTuplePolygonDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> inputTuples, Collector<PriorityQueue<Tuple2<Polygon, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Polygon poly : inputTuples) {
                            double distance;
                            if (kNNPQ.size() < k) {
                                if(approximateQuery) {
                                    distance = HelperClass.getPointPolygonBBoxMinEuclideanDistance(queryPoint, poly);
                                }else{
                                    distance = DistanceFunctions.getDistance(queryPoint, poly);
                                }
                                kNNPQ.offer(new Tuple2<Polygon, Double>(poly, distance));
                            } else {
                                if(approximateQuery) {
                                    distance = HelperClass.getPointPolygonBBoxMinEuclideanDistance(queryPoint, poly);
                                }else{
                                    distance = DistanceFunctions.getDistance(queryPoint, poly);
                                }
                                //double largestDistInPQ = HelperClass.getPointPolygonMinEuclideanDistance(queryPoint, kNNPQ.peek().f0);
                                // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                double largestDistInPQ = kNNPQ.peek().f1;

                                if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                    kNNPQ.poll();
                                    kNNPQ.offer(new Tuple2<Polygon, Double>(poly, distance));
                                }
                            }
                        }

                        // Output stream
                        outputStream.collect(kNNPQ);
                    }
                }).name("Windowed (Apply) Grid Based");


        // windowAll to Generate integrated kNN -
        DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> windowAllKNN = windowedKNN
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize),Time.seconds(windowSlideStep)))
                .apply(new AllWindowFunction<PriorityQueue<Tuple2<Polygon, Double>>, Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>, TimeWindow>() {

                    PriorityQueue<Tuple2<Polygon, Double>> kNNPQWinAll = new PriorityQueue<Tuple2<Polygon, Double>>(k, new Comparators.inTuplePolygonDistanceComparator());
                    Set<String> objIDs = new HashSet<String>();

                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<PriorityQueue<Tuple2<Polygon, Double>>> input, Collector<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> output) throws Exception {
                        kNNPQWinAll.clear();
                        objIDs.clear();
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
                                            if(existingPQTuple.f0.objID == candidPQTuple.f0.objID && existingPQTuple.f1 > candidPQTuple.f1){
                                                kNNPQWinAll.remove(existingPQTuple);
                                                kNNPQWinAll.offer(candidPQTuple);
                                                break;
                                            }
                                        }
                                    }
                                }
                                // If there are already required (k) number of tuples in kNNPQWinAll
                                else{
                                    double largestDistInkNNPQ = kNNPQWinAll.peek().f1;
                                    if (largestDistInkNNPQ > candidPQTuple.f1) {
                                        // Add an object if it is not already there
                                        if(!objIDs.contains(candidPQTuple.f0.objID)) {
                                            // remove element with the largest distance and add the new element
                                            kNNPQWinAll.poll();
                                            objIDs.remove(kNNPQWinAll.peek().f0.objID);

                                            kNNPQWinAll.offer(candidPQTuple);
                                            objIDs.add(candidPQTuple.f0.objID);
                                        }
                                        else {
                                            // (To avoid duplicate addition of an object in kNN) Object is already in PQ, check the existing object's distance compared to current object
                                            for (Tuple2<Polygon, Double> existingPQTuple : kNNPQWinAll) {
                                                if (existingPQTuple.f0.objID == candidPQTuple.f0.objID && existingPQTuple.f1 > candidPQTuple.f1) {
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
                });

        //Output kNN Stream
        return windowAllKNN;
    }




    //--------------- GRID-BASED kNN QUERY - POLYGON-POLYGON -----------------//
    public static DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> SpatialKNNQuery(DataStream<Polygon> polygonStream, Polygon queryPolygon, double queryRadius, Integer k, UniformGrid uGrid, int windowSize, int windowSlideStep) throws IOException {

        // Generate a replicated polygon stream to know the grid IDs of each polygon
        DataStream<Polygon> replicatedPolygonStream = polygonStream.flatMap(new HelperClass.ReplicatePolygonStream());

        // Compute the neighboring layers cells for filtering
        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);

        // Filter out the polygons which lie greater than queryRadius of the query point
        DataStream<Polygon> filteredPolygons = replicatedPolygonStream.filter(new FilterFunction<Polygon>() {
            @Override
            public boolean filter(Polygon poly) throws Exception {
                return ((candidateNeighboringCells.contains(poly.gridID)) || (guaranteedNeighboringCells.contains(poly.gridID)));
            }
        });

        DataStream<PriorityQueue<Tuple2<Polygon, Double>>> windowedKNN = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
            @Override
            public String getKey(Polygon poly) throws Exception {
                return poly.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Polygon, PriorityQueue<Tuple2<Polygon, Double>>, String, TimeWindow>() {

                    PriorityQueue<Tuple2<Polygon, Double>> kNNPQ = new PriorityQueue<Tuple2<Polygon, Double>>(k, new Comparators.inTuplePolygonDistanceComparator());

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> inputTuples, Collector<PriorityQueue<Tuple2<Polygon, Double>>> outputStream) throws Exception {
                        kNNPQ.clear();

                        for (Polygon poly : inputTuples) {

                            if (kNNPQ.size() < k) {
                                double distance = HelperClass.getBBoxBBoxMinEuclideanDistance(queryPolygon.boundingBox, poly.boundingBox);
                                kNNPQ.offer(new Tuple2<Polygon, Double>(poly, distance));
                            } else {
                                double distance = HelperClass.getBBoxBBoxMinEuclideanDistance(queryPolygon.boundingBox, poly.boundingBox);
                                //double largestDistInPQ = HelperClass.getPointPolygonMinEuclideanDistance(queryPoint, kNNPQ.peek().f0);
                                // PQ is maintained in descending order with the object with the largest distance from query point at the top/peek
                                double largestDistInPQ = kNNPQ.peek().f1;

                                if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                    kNNPQ.poll();
                                    kNNPQ.offer(new Tuple2<Polygon, Double>(poly, distance));
                                }
                            }
                        }



                        // Output stream
                        outputStream.collect(kNNPQ);
                    }
                }).name("Windowed (Apply) Grid Based");


        // windowAll to Generate integrated kNN -
        DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> windowAllKNN = windowedKNN
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize),Time.seconds(windowSlideStep)))
                .apply(new AllWindowFunction<PriorityQueue<Tuple2<Polygon, Double>>, Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>, TimeWindow>() {

                    PriorityQueue<Tuple2<Polygon, Double>> kNNPQWinAll = new PriorityQueue<Tuple2<Polygon, Double>>(k, new Comparators.inTuplePolygonDistanceComparator());
                    Set<String> objIDs = new HashSet<String>();

                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<PriorityQueue<Tuple2<Polygon, Double>>> input, Collector<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> output) throws Exception {
                        kNNPQWinAll.clear();
                        objIDs.clear();
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
                                            if(existingPQTuple.f0.objID == candidPQTuple.f0.objID && existingPQTuple.f1 > candidPQTuple.f1){
                                                kNNPQWinAll.remove(existingPQTuple);
                                                kNNPQWinAll.offer(candidPQTuple);
                                                break;
                                            }
                                        }
                                    }
                                }
                                // If there are already required (k) number of tuples in kNNPQWinAll
                                else{
                                    double largestDistInkNNPQ = kNNPQWinAll.peek().f1;
                                    if (largestDistInkNNPQ > candidPQTuple.f1) {
                                        // Add an object if it is not already there
                                        if(!objIDs.contains(candidPQTuple.f0.objID)) {
                                            // remove element with the largest distance and add the new element
                                            kNNPQWinAll.poll();
                                            objIDs.remove(kNNPQWinAll.peek().f0.objID);

                                            kNNPQWinAll.offer(candidPQTuple);
                                            objIDs.add(candidPQTuple.f0.objID);
                                        }
                                        else {
                                            // (To avoid duplicate addition of an object in kNN) Object is already in PQ, check the existing object's distance compared to current object
                                            for (Tuple2<Polygon, Double> existingPQTuple : kNNPQWinAll) {
                                                if (existingPQTuple.f0.objID == candidPQTuple.f0.objID && existingPQTuple.f1 > candidPQTuple.f1) {
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
                });

        //Output kNN Stream
        return windowAllKNN;
    }






}