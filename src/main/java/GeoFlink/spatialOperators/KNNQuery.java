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
import GeoFlink.utils.HelperClass;
import GeoFlink.utils.SpatialDistanceComparator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
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
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Set;

public class KNNQuery implements Serializable {

    public KNNQuery() {}

    // Grid-based approach - Iterative Distributed
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

                    PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new SpatialDistanceComparator(queryPoint));

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

                    PriorityQueue<Tuple2<Point, Double>> kNNPQWinAll = new PriorityQueue<Tuple2<Point, Double>>(k, new SpatialDistanceComparator(queryPoint));
                    PriorityQueue<Tuple2<Point, Double>> controlPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new SpatialDistanceComparator(queryPoint));


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


    public static DataStream<PriorityQueue<Tuple2<Point, Double>>> SpatialKNNQuery(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k, int windowSize, int windowSlideStep, UniformGrid uGrid) throws IOException {

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

                    PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new SpatialDistanceComparator(queryPoint));

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<PriorityQueue<Tuple2<Point, Double>>> outputStream) throws Exception {
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
                }).name("Windowed (Apply) Grid Based");


        // windowAll to Generate integrated kNN -
        DataStream<PriorityQueue<Tuple2<Point, Double>>> windowAllKNN = windowedKNN
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize),Time.seconds(windowSlideStep)))
                .apply(new AllWindowFunction<PriorityQueue<Tuple2<Point, Double>>, PriorityQueue<Tuple2<Point, Double>>, TimeWindow>() {

                    PriorityQueue<Tuple2<Point, Double>> kNNPQWinAll = new PriorityQueue<Tuple2<Point, Double>>(k, new SpatialDistanceComparator(queryPoint));

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
                    }
                });

        //Output kNN Stream
        return windowAllKNN;
    }
}