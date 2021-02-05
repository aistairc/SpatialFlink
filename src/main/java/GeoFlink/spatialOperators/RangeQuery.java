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
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Set;

public class RangeQuery implements Serializable {


    //--------------- GRID-BASED RANGE QUERY - POINT -----------------//
    public static DataStream<Point> SpatialRangeQuery(DataStream<Point> pointStream, Point queryPoint, double queryRadius, int windowSize, int slideStep, UniformGrid uGrid){

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint.gridID, guaranteedNeighboringCells);

        DataStream<Point> filteredPoints = pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
            }
        });

        DataStream<Point> rangeQueryNeighbours = filteredPoints.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new WindowFunction<Point, Point, String, TimeWindow>() {
                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> pointIterator, Collector<Point> neighbors) throws Exception {
                        for (Point point : pointIterator) {
                            if (guaranteedNeighboringCells.contains(point.gridID))
                                neighbors.collect(point);
                            else {
                                Double distance = HelperClass.getPointPointEuclideanDistance(queryPoint.point.getX(), queryPoint.point.getY(), point.point.getX(),point.point.getY());
                                if (distance <= queryRadius)
                                { neighbors.collect(point);}
                            }
                        }
                    }
                }).name("Windowed (Apply) Grid Based");

//        DataStream<Point> aggregatedNeighbours = rangeQueryNeighbours.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
//                .apply(new AllWindowFunction<Point, Point, TimeWindow>() {
//                    @Override
//                    public void apply(TimeWindow timeWindow, Iterable<Point> pointIterator, Collector<Point> neighbors) throws Exception {
//                        for (Point point : pointIterator) {
//                            neighbors.collect(point);
//                        }
//                    }
//                });
        return rangeQueryNeighbours;
    }

    //--------------- GRID-BASED RANGE QUERY - POINT - POLYGON -----------------//
    public static DataStream<Polygon> SpatialRangeQuery(DataStream<Polygon> polygonStream, Point queryPoint, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep ) {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint.gridID, guaranteedNeighboringCells);

        DataStream<Polygon> replicatedPolygonStream = polygonStream.flatMap(new HelperClass.ReplicatePolygonStream());

        // Filtering out the polygons which lie greater than queryRadius of the query point
        DataStream<Polygon> filteredPolygons = replicatedPolygonStream.filter(new FilterFunction<Polygon>() {
            @Override
            public boolean filter(Polygon poly) throws Exception {
                return ((candidateNeighboringCells.contains(poly.gridID)) || (guaranteedNeighboringCells.contains(poly.gridID)));
            }
        });

        DataStream<Polygon> rangeQueryNeighbours = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
            @Override
            public String getKey(Polygon poly) throws Exception {
                return poly.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new WindowFunction<Polygon, Polygon, String, TimeWindow>() {
                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> pointIterator, Collector<Polygon> neighbors) throws Exception {
                        for (Polygon poly : pointIterator) {
                            if (guaranteedNeighboringCells.contains(poly.gridID))
                                neighbors.collect(poly);
                            else {
                                //Double distance = HelperClass.computeEuclideanDistance(queryPoint.point.getX(), queryPoint.point.getY(), poly.point.getX(),poly.point.getY());
                                Double distance = HelperClass.getPointPolygonMinEuclideanDistance(queryPoint, poly);
                                //System.out.println("Distance: " + distance);
                                if (distance <= queryRadius){
                                    neighbors.collect(poly);
                                }
                            }
                        }
                    }
                }).name("Windowed (Apply) Grid Based");

        return rangeQueryNeighbours;
    }

    //--------------- GRID-BASED RANGE QUERY - POINT - LineString -----------------//
    public static DataStream<LineString> SpatialLineStringRangeQuery(DataStream<LineString> lineStringStream, Point queryPoint, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep ) {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint.gridID, guaranteedNeighboringCells);

        DataStream<LineString> replicatedLineStringStream = lineStringStream.flatMap(new HelperClass.ReplicateLineStringStream());

        // Filtering out the polygons which lie greater than queryRadius of the query point
        DataStream<LineString> filteredLineStrings = replicatedLineStringStream.filter(new FilterFunction<LineString>() {
            @Override
            public boolean filter(LineString lineString) throws Exception {
                return ((candidateNeighboringCells.contains(lineString.gridID)) || (guaranteedNeighboringCells.contains(lineString.gridID)));
            }
        });

        DataStream<LineString> rangeQueryNeighbours = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
            @Override
            public String getKey(LineString lineString) throws Exception {
                return lineString.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new WindowFunction<LineString, LineString, String, TimeWindow>() {
                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<LineString> pointIterator, Collector<LineString> neighbors) throws Exception {
                        for (LineString lineString : pointIterator) {
                            if (guaranteedNeighboringCells.contains(lineString.gridID))
                                neighbors.collect(lineString);
                            else {
                                //Double distance = HelperClass.computeEuclideanDistance(queryPoint.point.getX(), queryPoint.point.getY(), poly.point.getX(),poly.point.getY());
                                Double distance = HelperClass.getPointLineStringMinEuclideanDistance(queryPoint, lineString);
                                //System.out.println("Distance: " + distance);
                                if (distance <= queryRadius){
                                    neighbors.collect(lineString);
                                }
                            }
                        }
                    }
                }).name("Windowed (Apply) Grid Based");

        return rangeQueryNeighbours;
    }

    //--------------- GRID-BASED RANGE QUERY - POLYGON - POLYGON -----------------//
    public static DataStream<Polygon> SpatialRangeQuery(DataStream<Polygon> polygonStream, Polygon queryPolygon, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep ) {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);

        DataStream<Polygon> replicatedPolygonStream = polygonStream.flatMap(new HelperClass.ReplicatePolygonStream());

        // Filtering out the polygons which lie greater than queryRadius of the query point
        DataStream<Polygon> filteredPolygons = replicatedPolygonStream.filter(new FilterFunction<Polygon>() {
            @Override
            public boolean filter(Polygon poly) throws Exception {
                return ((candidateNeighboringCells.contains(poly.gridID)) || (guaranteedNeighboringCells.contains(poly.gridID)));
            }
        });

        DataStream<Polygon> rangeQueryNeighbours = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
            @Override
            public String getKey(Polygon poly) throws Exception {
                return poly.gridID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new WindowFunction<Polygon, Polygon, String, TimeWindow>() {
                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> pointIterator, Collector<Polygon> neighbors) throws Exception {
                        for (Polygon poly : pointIterator) {
                            if (guaranteedNeighboringCells.contains(poly.gridID))
                                neighbors.collect(poly);
                            else {
                                Double distance = HelperClass.getPolygonPolygonMinEuclideanDistance(queryPolygon, poly);
                                //System.out.println("Distance: " + distance);
                                if (distance <= queryRadius){
                                    neighbors.collect(poly);
                                }
                            }
                        }
                    }
                }).name("Windowed (Apply) Grid Based");

        return rangeQueryNeighbours;
    }


    /*
    // Generation of replicated polygon stream corresponding to each grid cell a polygon belongs
    public static class ReplicatePolygonStream extends RichFlatMapFunction<Polygon, Polygon> {

        private long parallelism;
        private long uniqueObjID;

        @Override
        public void open(Configuration parameters) {
            RuntimeContext ctx = getRuntimeContext();
            parallelism = ctx.getNumberOfParallelSubtasks();
            uniqueObjID = ctx.getIndexOfThisSubtask();
        }

        @Override
        public void flatMap(Polygon poly, Collector<Polygon> out) throws Exception {

            // Create duplicated polygon stream based on GridIDs
            for (String gridID: poly.gridIDsSet) {
                Polygon p = new Polygon(Arrays.asList(poly.polygon.getCoordinates()), uniqueObjID, poly.gridIDsSet, gridID, poly.boundingBox);
                out.collect(p);
            }

            // Generating unique ID for each polygon, so that all the replicated tuples are assigned the same unique id
            uniqueObjID += parallelism;
        }
    }
     */
}