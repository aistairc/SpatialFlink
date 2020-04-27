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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.PriorityQueue;

public class KNNQuery implements Serializable {

    public KNNQuery() {}

    // Grid-based approach - Iterative Distributed
    public static DataStream<PriorityQueue<Tuple2<Point, Double>>> SpatialKNNQuery(DataStream<Point> pointStream, Point queryPoint, Integer k, int windowSize, int windowSlideStep, UniformGrid uGrid) throws IOException {

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
                if(p.objID == -99999){
                    filterationNeighboringLayers = uGrid.getCandidateNeighboringLayers(p.point.getX() * queryRadiusMultFactor);
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

        DataStream<PriorityQueue<Tuple2<Point, Double>>> windowedIterativeStream = filteredStream.keyBy("gridID")
                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize),Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Point, PriorityQueue<Tuple2<Point, Double>>, Tuple, TimeWindow>() {

                    PriorityQueue<Tuple2<Point, Double>> kNNPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new SpatialDistanceComparator(queryPoint));
                    PriorityQueue<Tuple2<Point, Double>> controlPQ = new PriorityQueue<Tuple2<Point, Double>>(k, new SpatialDistanceComparator(queryPoint));

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<PriorityQueue<Tuple2<Point, Double>>> outputStream) throws Exception {
                        String feedbackTupleKey = "";
                        kNNPQ.clear();

                        for (Point p : inputTuples) {

                            if (kNNPQ.size() < k) {
                                double distance = HelperClass.computeEuclideanDistance(p.point.getX(), p.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
                                kNNPQ.offer(new Tuple2<Point, Double>(p, distance));
                                feedbackTupleKey = p.gridID;
                            } else {
                                double distance = HelperClass.computeEuclideanDistance(p.point.getX(), p.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
                                //assert kNNPQ.peek() != null; // break program if the condition is true
                                double largestDistInPQ = HelperClass.computeEuclideanDistance(kNNPQ.peek().f0.point.getX(), kNNPQ.peek().f0.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());

                                if (largestDistInPQ > distance) { // remove element with the largest distance and add the new element
                                    kNNPQ.poll();
                                    kNNPQ.offer(new Tuple2<Point, Double>(p, distance));
                                }
                            }
                        }

                        // Output stream
                        outputStream.collect(kNNPQ);

                        double largestDistInkNNPQ = HelperClass.computeEuclideanDistance(kNNPQ.peek().f0.point.getX(), kNNPQ.peek().f0.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
                        Point feedbackTuple = new Point(-99999, largestDistInkNNPQ, largestDistInkNNPQ, feedbackTupleKey);
                        controlPQ.offer(new Tuple2<Point, Double>(feedbackTuple, -99999.99999));

                        // Adding the control tuple
                        outputStream.collect(controlPQ);
                    }
                });

        // Assuming that the distance between two objects is always >= 0
        // Feedback
        DataStream<Point> feedbackStream = windowedIterativeStream.flatMap(new FlatMapFunction<PriorityQueue<Tuple2<Point, Double>>, Point>() {
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
        return windowedIterativeStream.filter(new FilterFunction<PriorityQueue<Tuple2<Point, Double>>>() {
            @Override
            public boolean filter(PriorityQueue<Tuple2<Point, Double>> inputTuple) throws Exception {
                return (inputTuple.peek().f1 >= 0);
            }
        });
    }
}