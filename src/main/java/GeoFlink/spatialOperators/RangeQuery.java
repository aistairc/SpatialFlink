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
import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.utils.DistanceFunctions;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RangeQuery {

    public static abstract class AbstractRangeQuery implements Serializable {
    }

    public static class PointRangeQuery extends AbstractRangeQuery {
        // ********* Real-time Queries ********* //
        //--------------- Real-time - POINT - POINT -----------------//
        public static DataStream<Point> RangeQuery(DataStream <Point> pointStream, Point queryPoint,
                                                   double queryRadius, UniformGrid uGrid, boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint.gridID, guaranteedNeighboringCells);

            //pointStream.print();
            DataStream<Point> filteredPoints = pointStream.filter(new FilterFunction<Point>() {
                @Override
                public boolean filter(Point point) throws Exception {
                    return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
                }
            }).startNewChain();


            DataStream<Point> rangeQueryNeighbours = filteredPoints.keyBy(new KeySelector<Point, String>() {
                @Override
                public String getKey(Point p) throws Exception {
                    return p.gridID;
                }
            }).flatMap(new FlatMapFunction<Point, Point>() {
                @Override
                public void flatMap(Point point, Collector<Point> collector) throws Exception {

                    if (guaranteedNeighboringCells.contains(point.gridID)) {
                        collector.collect(point);
                    } else {

                        if (approximateQuery) { // all the candidate neighbors are sent to output
                            collector.collect(point);
                        } else {
                            Double distance = DistanceFunctions.getDistance(queryPoint, point);

                            if (distance <= queryRadius) {
                                collector.collect(point);
                            }
                        }

                    }
                }
            }).name("Real-time - POINT - POINT");

            return rangeQueryNeighbours;
        }

        //--------------- Real-time - POLYGON - POINT -----------------//
        public static DataStream<Point> RangeQuery(DataStream < Point > pointStream, Polygon queryPolygon,
                                                   double queryRadius, UniformGrid uGrid, boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);

            DataStream<Point> filteredPoints = pointStream.filter(new FilterFunction<Point>() {
                @Override
                public boolean filter(Point point) throws Exception {
                    return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
                }
            }).startNewChain();

            DataStream<Point> rangeQueryNeighbours = filteredPoints.keyBy(new KeySelector<Point, String>() {
                @Override
                public String getKey(Point p) throws Exception {
                    return p.gridID;
                }
            }).flatMap(new FlatMapFunction<Point, Point>() {
                @Override
                public void flatMap(Point point, Collector<Point> collector) throws Exception {

                    if (guaranteedNeighboringCells.contains(point.gridID)) {
                        collector.collect(point);
                    } else {

                        Double distance;
                        if (approximateQuery) {
                            distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                        } else {
                            distance = DistanceFunctions.getDistance(point, queryPolygon);
                        }

                        if (distance <= queryRadius) {
                            collector.collect(point);
                        }
                    }

                }
            }).name("Real-time - POLYGON - POINT");

            return rangeQueryNeighbours;
        }

        //--------------- Real-time - POLYGON - POINT -----------------//
        public static DataStream<Long> RangeQueryLatency(DataStream < Point > pointStream, Polygon queryPolygon,
                                                         double queryRadius, UniformGrid uGrid, boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);

            final OutputTag<String> outputTag = new OutputTag<String>("side-output") {
            };

            DataStream<Point> filteredPoints = pointStream.filter(new FilterFunction<Point>() {
                @Override
                public boolean filter(Point point) throws Exception {
                    return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
                }
            }).startNewChain();

            DataStream<Long> rangeQueryNeighbours = filteredPoints.keyBy(new KeySelector<Point, String>() {
                @Override
                public String getKey(Point p) throws Exception {
                    return p.gridID;
                }
            }).flatMap(new FlatMapFunction<Point, Long>() {
                @Override
                public void flatMap(Point point, Collector<Long> collector) throws Exception {

                    if (guaranteedNeighboringCells.contains(point.gridID)) {
                        Date date = new Date();
                        Long latency = date.getTime() - point.timeStampMillisec;
                        collector.collect(latency);
                    } else {

                        Double distance;
                        if (approximateQuery) {
                            distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                        } else {
                            distance = DistanceFunctions.getDistance(point, queryPolygon);
                        }
                        Date date = new Date();
                        Long latency = date.getTime() - point.timeStampMillisec;
                        collector.collect(latency);
                    }

                }
            }).name("Real-time - POLYGON - POINT");

            return rangeQueryNeighbours;
        }

        //--------------- Real-time - LINESTRING - POINT -----------------//
        public static DataStream<Point> RangeQuery(DataStream < Point > pointStream, LineString queryLineString,
                                                   double queryRadius, UniformGrid uGrid, boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryLineString);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryLineString, guaranteedNeighboringCells);

            DataStream<Point> filteredPoints = pointStream.filter(new FilterFunction<Point>() {
                @Override
                public boolean filter(Point point) throws Exception {
                    return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
                }
            }).startNewChain();

            DataStream<Point> rangeQueryNeighbours = filteredPoints.keyBy(new KeySelector<Point, String>() {
                @Override
                public String getKey(Point p) throws Exception {
                    return p.gridID;
                }
            }).flatMap(new FlatMapFunction<Point, Point>() {
                @Override
                public void flatMap(Point point, Collector<Point> collector) throws Exception {

                    if (guaranteedNeighboringCells.contains(point.gridID))
                        collector.collect(point);
                    else {

                        Double distance;
                        if (approximateQuery) {
                            distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(point, queryLineString);
                        } else {
                            distance = DistanceFunctions.getDistance(point, queryLineString);
                        }

                        if (distance <= queryRadius) {
                            collector.collect(point);
                        }
                    }
                }
            }).name("Real-time - LINESTRING - POINT");

            return rangeQueryNeighbours;
        }
        // ********* Real-time Queries End ********* //


        // ********* Window-based Queries ********* //
        //--------------- Window-based - POINT - POINT -----------------//
        public static DataStream<Point> PointRangeQueryIncremental (DataStream < Point > pointStream, Point queryPoint,
                                                                    double queryRadius, UniformGrid uGrid,int windowSize, int slideStep, int allowedLateness,
                                                                    boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint.gridID, guaranteedNeighboringCells);

            // Spatial stream with Timestamps and Watermarks
            // Max Allowed Lateness: allowedLateness
            DataStream<Point> pointStreamWithTsAndWm =
                    pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(Point p) {
                            return p.timeStampMillisec;
                        }
                    }).startNewChain();

            DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
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
            }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .apply(new RichWindowFunction<Point, Point, String, TimeWindow>() {

                        /**
                         * The ListState handle.
                         */
                        private transient ListState<Point> queryOutputListState;

                        @Override
                        public void open(Configuration config) {

                            PojoTypeInfo<Point> pointTypeInfo = (PojoTypeInfo<Point>) TypeInformation.of(Point.class);

                            ListStateDescriptor<Point> queryOutputStateDescriptor = new ListStateDescriptor<Point>(
                                    "queryOutputStateDescriptor",// state name
                                    pointTypeInfo);
                            //TypeInformation.of(TypeHint)
                            this.queryOutputListState = getRuntimeContext().getListState(queryOutputStateDescriptor);
                        }

                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> pointIterator, Collector<Point> neighbors) throws Exception {

                            List<Point> nextWindowUsefulOutputFromPastWindow = new ArrayList<>();
                            // Access the list state - past output
                            for (Point point : queryOutputListState.get()) {
                                //System.out.println("state " + point);
                                neighbors.collect(point);

                                // Storing points useful for next window
                                if (point.timeStampMillisec >= (timeWindow.getStart() + (slideStep * 1000))) {
                                    nextWindowUsefulOutputFromPastWindow.add(point);
                                }
                            }

                            // Clear the list state
                            queryOutputListState.clear();
                            // Populating the list state with the points useful for next window
                            queryOutputListState.addAll(nextWindowUsefulOutputFromPastWindow);

                            for (Point point : pointIterator) {
                                // Check for Range Query only for new objects
                                if (point.timeStampMillisec >= (timeWindow.getEnd() - (slideStep * 1000))) {

                                    //System.out.println(point);
                                    if (guaranteedNeighboringCells.contains(point.gridID)) {
                                        neighbors.collect(point);
                                        queryOutputListState.add(point); // add new output useful for next window
                                    } else {

                                        if (approximateQuery) { // all the candidate neighbors are sent to output
                                            neighbors.collect(point);
                                            queryOutputListState.add(point); // add new output useful for next window
                                        } else {
                                            Double distance = DistanceFunctions.getDistance(queryPoint, point);
                                            //System.out.println("distance " + distance);

                                            if (distance <= queryRadius) {
                                                neighbors.collect(point);
                                                queryOutputListState.add(point); // add new output useful for next window
                                            }
                                        }
                                    }

                                }
                            }
                        }
                    }).name("Window-based - POINT - POINT");

            return rangeQueryNeighbours;
        }

        //--------------- Window-based - POINT - POINT -----------------//
        public static DataStream<Point> RangeQuery(DataStream < Point > pointStream, Point queryPoint,
                                                   double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness,
                                                   boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint.gridID, guaranteedNeighboringCells);

            DataStream<Point> pointStreamWithTsAndWm =
                    pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(Point p) {
                            return p.timeStampMillisec;
                        }
                    }).startNewChain();

            DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
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

                                    if (approximateQuery) { // all the candidate neighbors are sent to output
                                        neighbors.collect(point);
                                    } else {
                                        Double distance = DistanceFunctions.getDistance(queryPoint, point);

                                        if (distance <= queryRadius) {
                                            neighbors.collect(point);
                                        }
                                    }
                                }
                            }
                        }
                    }).name("Window-based - POINT - POINT");

            return rangeQueryNeighbours;
        }

        //--------------- Window-based - POLYGON - POINT -----------------//
        public static DataStream<Point> RangeQuery(DataStream < Point > pointStream, Polygon queryPolygon,
                                                   double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness,
                                                   boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);

            DataStream<Point> pointStreamWithTsAndWm =
                    pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(Point p) {
                            return p.timeStampMillisec;
                        }
                    }).startNewChain();

            DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
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

                                    Double distance;
                                    if (approximateQuery) {
                                        distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                                    } else {
                                        distance = DistanceFunctions.getDistance(point, queryPolygon);
                                    }

                                    if (distance <= queryRadius) {
                                        neighbors.collect(point);
                                    }
                                }
                            }
                        }
                    }).name("Window-based - POLYGON - POINT");

            return rangeQueryNeighbours;
        }

        //--------------- Window-based - POLYGON - POINT -----------------//
        public static DataStream<Long> RangeQueryLatency(DataStream < Point > pointStream, Polygon queryPolygon,
                                                         double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness,
                                                         boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);

            DataStream<Point> pointStreamWithTsAndWm =
                    pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(Point p) {
                            return p.timeStampMillisec;
                        }
                    }).startNewChain();

            DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
                @Override
                public boolean filter(Point point) throws Exception {
                    return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
                }
            });

            DataStream<Long> rangeQueryNeighbours = filteredPoints.keyBy(new KeySelector<Point, String>() {
                @Override
                public String getKey(Point p) throws Exception {
                    return p.gridID;
                }
            }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .apply(new WindowFunction<Point, Long, String, TimeWindow>() {
                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> pointIterator, Collector<Long> neighbors) throws Exception {
                            for (Point point : pointIterator) {
                                if (guaranteedNeighboringCells.contains(point.gridID)) {
                                    Date date = new Date();
                                    Long latency = date.getTime() - point.timeStampMillisec;
                                    neighbors.collect(latency);
                                } else {

                                    Double distance;
                                    if (approximateQuery) {
                                        distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, queryPolygon);
                                    } else {
                                        distance = DistanceFunctions.getDistance(point, queryPolygon);
                                    }
                                    Date date = new Date();
                                    Long latency = date.getTime() - point.timeStampMillisec;
                                    neighbors.collect(latency);
                                }
                            }
                        }
                    }).name("Window-based - POLYGON - POINT");

            return rangeQueryNeighbours;
        }

        //--------------- Window-based - LINESTRING - POINT -----------------//
        public static DataStream<Point> RangeQuery(DataStream < Point > pointStream, LineString queryLineString,
                                                   double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness,
                                                   boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryLineString);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryLineString, guaranteedNeighboringCells);

            DataStream<Point> pointStreamWithTsAndWm =
                    pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(Point p) {
                            return p.timeStampMillisec;
                        }
                    }).startNewChain();

            DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
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

                                    Double distance;
                                    if (approximateQuery) {
                                        distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(point, queryLineString);
                                    } else {
                                        distance = DistanceFunctions.getDistance(point, queryLineString);
                                    }

                                    if (distance <= queryRadius) {
                                        neighbors.collect(point);
                                    }
                                }
                            }
                        }
                    }).name("Window-based - LINESTRING - POINT");

            return rangeQueryNeighbours;
        }
        // ********* Window-based Queries End ********* //
    }

    public static class PolygonRangeQuery extends AbstractRangeQuery {
        // ********* Real-time Queries ********* //
        //--------------- Real-time - POINT - POLYGON -----------------//
        public static DataStream<Polygon> RangeQuery(DataStream < Polygon > polygonStream, Point queryPoint,
                                                     double queryRadius, UniformGrid uGrid, boolean approximateQuery){

            Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);
            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = polygonStream.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells)).startNewChain();

            DataStream<Polygon> rangeQueryNeighbours = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).flatMap(new FlatMapFunction<Polygon, Polygon>() {
                @Override
                public void flatMap(Polygon poly, Collector<Polygon> collector) throws Exception {

                    //System.out.println(poly);

                    int cellIDCounter = 0;
                    for (String polyGridID : poly.gridIDsSet) {

                        if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                            cellIDCounter++;
                            // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                            if (cellIDCounter == poly.gridIDsSet.size()) {
                                collector.collect(poly);
                            }
                        } else { // candidate neighbors

                            Double distance;
                            if (approximateQuery) {
                                distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(queryPoint, poly);
                            } else {
                                distance = DistanceFunctions.getDistance(queryPoint, poly);
                            }

                            if (distance <= queryRadius) {
                                collector.collect(poly);
                            }
                            break;
                        }
                    }

                }
            }).name("Real-time - POINT - POLYGON");

            return rangeQueryNeighbours;
        }

        //--------------- Real-time - POLYGON - POLYGON -----------------//
        public static DataStream<Polygon> RangeQuery(DataStream < Polygon > polygonStream, Polygon queryPolygon,
                                                     double queryRadius, UniformGrid uGrid, boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(), candidateNeighboringCells.stream()).collect(Collectors.toSet());

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = polygonStream.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells)).startNewChain();

            DataStream<Polygon> rangeQueryNeighbours = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).flatMap(new FlatMapFunction<Polygon, Polygon>() {
                @Override
                public void flatMap(Polygon poly, Collector<Polygon> collector) throws Exception {

                    int cellIDCounter = 0;
                    for (String polyGridID : poly.gridIDsSet) {

                        if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                            cellIDCounter++;
                            // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                            if (cellIDCounter == poly.gridIDsSet.size()) {
                                collector.collect(poly);
                            }
                        } else { // candidate neighbors
                            Double distance;
                            if (approximateQuery) {
                                distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryPolygon.boundingBox, poly.boundingBox);
                            } else {
                                distance = DistanceFunctions.getDistance(poly, queryPolygon);
                            }

                            if (distance <= queryRadius) {
                                collector.collect(poly);
                            }
                            break;
                        }
                    }

                }
            }).name("Real-time - POINT - POLYGON");

            return rangeQueryNeighbours;
        }

        //--------------- Real-time - LINESTRING - POLYGON -----------------//
        public static DataStream<Polygon> RangeQuery(DataStream < Polygon > polygonStream, LineString
                queryLineString, double queryRadius, UniformGrid uGrid, boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryLineString);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryLineString, guaranteedNeighboringCells);
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(), candidateNeighboringCells.stream()).collect(Collectors.toSet());

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = polygonStream.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells)).startNewChain();

            DataStream<Polygon> rangeQueryNeighbours = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).flatMap(new FlatMapFunction<Polygon, Polygon>() {
                @Override
                public void flatMap(Polygon poly, Collector<Polygon> collector) throws Exception {

                    int cellIDCounter = 0;
                    for (String polyGridID : poly.gridIDsSet) {

                        if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                            cellIDCounter++;
                            // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                            if (cellIDCounter == poly.gridIDsSet.size()) {
                                collector.collect(poly);
                            }
                        } else { // candidate neighbors

                            Double distance;
                            if (approximateQuery) {
                                distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(poly.boundingBox, queryLineString.boundingBox);
                            } else {
                                distance = DistanceFunctions.getDistance(poly, queryLineString);
                            }

                            if (distance <= queryRadius) {
                                collector.collect(poly);
                            }
                            break;
                        }
                    }

                }
            }).name("Real-time - LINESTRING - POLYGON");

            return rangeQueryNeighbours;
        }
        // ********* Real-time Queries End ********* //


        // ********* Window-based Queries ********* //
        //--------------- Window-based - POINT - POLYGON - Incremental -----------------//
        public static DataStream<Polygon> RangeQueryIncremental(DataStream < Polygon > polygonStream, Point
                queryPoint, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness,
                                                                boolean approximateQuery){

            Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);
            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);

            DataStream<Polygon> streamWithTsAndWm =
                    polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(Polygon p) {
                            return p.timeStampMillisec;
                        }
                    }).startNewChain();

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = streamWithTsAndWm.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells));
            DataStream<Polygon> rangeQueryNeighbours = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .trigger(new polygonTrigger(slideStep))
                    .apply(new RichWindowFunction<Polygon, Polygon, String, TimeWindow>() {

                        /**
                         * The ListState handle.
                         */
                        private transient ListState<Polygon> queryOutputListState;

                        @Override
                        public void open(Configuration config) {
                            PojoTypeInfo<Polygon> objTypeInfo = (PojoTypeInfo<Polygon>) TypeInformation.of(Polygon.class);

                            ListStateDescriptor<Polygon> queryOutputStateDescriptor = new ListStateDescriptor<Polygon>(
                                    "queryOutputStateDescriptor",// state name
                                    objTypeInfo);
                            //TypeInformation.of(new TypeHint<Point>() {})
                            this.queryOutputListState = getRuntimeContext().getListState(queryOutputStateDescriptor);
                        }


                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> pointIterator, Collector<Polygon> neighbors) throws Exception {

                            List<Polygon> nextWindowUsefulOutputFromPastWindow = new ArrayList<>();
                            // Access the list state - past output
                            for (Polygon obj : queryOutputListState.get()) {
                                //System.out.println("state " + point);
                                neighbors.collect(obj);

                                // Storing points useful for next window
                                if (obj.timeStampMillisec >= (timeWindow.getStart() + (slideStep * 1000))) {
                                    nextWindowUsefulOutputFromPastWindow.add(obj);
                                }
                            }

                            // Clear the list state
                            queryOutputListState.clear();
                            // Populating the list state with the points useful for next window
                            queryOutputListState.addAll(nextWindowUsefulOutputFromPastWindow);

                            for (Polygon poly : pointIterator) {
                                //System.out.println(poly);
                                // Check for Range Query only for new objects
                                if (poly.timeStampMillisec >= (timeWindow.getEnd() - (slideStep * 1000))) {
                                    //if(true) {

                                    int cellIDCounter = 0;
                                    for (String polyGridID : poly.gridIDsSet) {

                                        if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                                            cellIDCounter++;
                                            // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                            if (cellIDCounter == poly.gridIDsSet.size()) {
                                                neighbors.collect(poly);
                                                queryOutputListState.add(poly); // add new output useful for next window
                                            }
                                        } else { // candidate neighbors

                                            Double distance;
                                            if (approximateQuery) {
                                                distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(queryPoint, poly);
                                            } else {
                                                // https://locationtech.github.io/jts/javadoc/ (Euclidean Distance)
                                                //distance = poly.polygon.distance(queryPoint.point);
                                                distance = DistanceFunctions.getDistance(queryPoint, poly);
                                            }

                                            if (distance <= queryRadius) {
                                                neighbors.collect(poly);
                                                queryOutputListState.add(poly); // add new output useful for next window
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }).name("Window-based - POINT - POLYGON");

            return rangeQueryNeighbours;
        }

        //--------------- Window-based - POINT - POLYGON -----------------//
        public static DataStream<Polygon> RangeQuery(DataStream < Polygon > polygonStream, Point queryPoint,
                                                     double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness,
                                                     boolean approximateQuery){

            Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);
            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);

            DataStream<Polygon> streamWithTsAndWm =
                    polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(Polygon p) {
                            //System.out.println("timeStampMillisec " + p.timeStampMillisec);
                            return p.timeStampMillisec;
                        }
                    }).startNewChain();

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = streamWithTsAndWm.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells));

            DataStream<Polygon> rangeQueryNeighbours = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .apply(new WindowFunction<Polygon, Polygon, String, TimeWindow>() {
                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> pointIterator, Collector<Polygon> neighbors) throws Exception {
                            for (Polygon poly : pointIterator) {
                                int cellIDCounter = 0;
                                for (String polyGridID : poly.gridIDsSet) {

                                    if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                                        cellIDCounter++;
                                        // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                        if (cellIDCounter == poly.gridIDsSet.size()) {
                                            neighbors.collect(poly);
                                        }
                                    } else { // candidate neighbors

                                        Double distance;
                                        if (approximateQuery) {
                                            distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(queryPoint, poly);
                                        } else {
                                            //distance = HelperClass.getPointPolygonMinEuclideanDistance(queryPoint, poly);
                                            //System.out.println("HelperClass Dist: " + distance);
                                            // https://locationtech.github.io/jts/javadoc/ (Euclidean Distance)
                                            //distance = poly.polygon.distance(queryPoint.point);
                                            distance = DistanceFunctions.getDistance(queryPoint, poly);
                                            //System.out.println("LocationTech Dist: " + distance);
                                        }

                                        if (distance <= queryRadius) {
                                            neighbors.collect(poly);
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }).name("Window-based - POINT - POLYGON");

            return rangeQueryNeighbours;
        }

        //--------------- WINDOW-based - POLYGON - POLYGON -----------------//
        public static DataStream<Polygon> RangeQuery(DataStream < Polygon > polygonStream, Polygon queryPolygon,
                                                     double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness,
                                                     boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(), candidateNeighboringCells.stream()).collect(Collectors.toSet());

            DataStream<Polygon> streamWithTsAndWm =
                    polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(Polygon p) {
                            return p.timeStampMillisec;
                        }
                    }).startNewChain();

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = streamWithTsAndWm.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells));

            DataStream<Polygon> rangeQueryNeighbours = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .apply(new WindowFunction<Polygon, Polygon, String, TimeWindow>() {
                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> polygonIterator, Collector<Polygon> neighbors) throws Exception {
                            for (Polygon poly : polygonIterator) {
                                int cellIDCounter = 0;
                                for (String polyGridID : poly.gridIDsSet) {

                                    if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                                        cellIDCounter++;
                                        // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                        if (cellIDCounter == poly.gridIDsSet.size()) {
                                            neighbors.collect(poly);
                                        }
                                    } else { // candidate neighbors
                                        Double distance;
                                        if (approximateQuery) {
                                            distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryPolygon.boundingBox, poly.boundingBox);
                                        } else {
                                            distance = DistanceFunctions.getDistance(poly, queryPolygon);
                                        }

                                        if (distance <= queryRadius) {
                                            neighbors.collect(poly);
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }).name("Window-based - POINT - POLYGON");

            return rangeQueryNeighbours;
        }

        //--------------- WINDOW-based - LINESTRING - POLYGON -----------------//
        public static DataStream<Polygon> RangeQuery(DataStream < Polygon > polygonStream, LineString
                queryLineString, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness,
                                                     boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryLineString);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryLineString, guaranteedNeighboringCells);
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(), candidateNeighboringCells.stream()).collect(Collectors.toSet());

            DataStream<Polygon> streamWithTsAndWm =
                    polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(Polygon p) {
                            return p.timeStampMillisec;
                        }
                    }).startNewChain();

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = streamWithTsAndWm.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells));

            DataStream<Polygon> rangeQueryNeighbours = filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .apply(new WindowFunction<Polygon, Polygon, String, TimeWindow>() {
                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> polygonIterator, Collector<Polygon> neighbors) throws Exception {

                            for (Polygon poly : polygonIterator) {
                                int cellIDCounter = 0;
                                for (String polyGridID : poly.gridIDsSet) {

                                    if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                                        cellIDCounter++;
                                        // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                        if (cellIDCounter == poly.gridIDsSet.size()) {
                                            neighbors.collect(poly);
                                        }
                                    } else { // candidate neighbors

                                        Double distance;
                                        if (approximateQuery) {
                                            distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(poly.boundingBox, queryLineString.boundingBox);
                                        } else {
                                            distance = DistanceFunctions.getDistance(poly, queryLineString);
                                        }

                                        if (distance <= queryRadius) {
                                            neighbors.collect(poly);
                                        }
                                        break;
                                    }
                                }
                            }

                        }
                    }).name("Window-based - LINESTRING - POLYGON");

            return rangeQueryNeighbours;
        }
        // ********* Window-based Queries End ********* //
    }

    public static class LineStringRangeQuery extends AbstractRangeQuery {
        // ********* Real-time Queries ********* //
        //--------------- Real-time - POINT - LineString -----------------//
        public static DataStream<LineString> RangeQuery(DataStream < LineString > lineStringStream, Point
                queryPoint, double queryRadius, UniformGrid uGrid, boolean approximateQuery){

            Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);
            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);

            // Filtering out the linestrings which lie greater than queryRadius of the query point
            DataStream<LineString> filteredLineStrings = lineStringStream.flatMap(new cellBasedLineStringFlatMap(neighboringCells)).startNewChain();

            DataStream<LineString> rangeQueryNeighbours = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
                @Override
                public String getKey(LineString lineString) throws Exception {
                    return lineString.gridID;
                }
            }).flatMap(new FlatMapFunction<LineString, LineString>() {
                @Override
                public void flatMap(LineString lineString, Collector<LineString> collector) throws Exception {

                    int cellIDCounter = 0;
                    for (String lineStringGridID : lineString.gridIDsSet) {
                        if (guaranteedNeighboringCells.contains(lineStringGridID)) {
                            cellIDCounter++;
                            // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                            if (cellIDCounter == lineString.gridIDsSet.size()) {
                                collector.collect(lineString);
                            }
                        } else { // candidate neighbors

                            Double distance;
                            if (approximateQuery) {
                                distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(queryPoint, lineString);
                            } else {
                                distance = DistanceFunctions.getDistance(queryPoint, lineString);
                            }

                            if (distance <= queryRadius) {
                                collector.collect(lineString);
                            }
                            break;
                        }
                    }

                }
            }).name("Real-time - POINT - LINESTRING");

            return rangeQueryNeighbours;
        }

        //--------------- Real-time - LINESTRING - POLYGON -----------------//
        public static DataStream<LineString> RangeQuery(DataStream < LineString > lineStringStream, Polygon
                queryPolygon, double queryRadius, UniformGrid uGrid, boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(), candidateNeighboringCells.stream()).collect(Collectors.toSet());

            DataStream<LineString> filteredLineStrings = lineStringStream.flatMap(new cellBasedLineStringFlatMap(neighboringCells)).startNewChain();

            DataStream<LineString> rangeQueryNeighbours = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
                @Override
                public String getKey(LineString lineString) throws Exception {
                    return lineString.gridID;
                }
            }).flatMap(new FlatMapFunction<LineString, LineString>() {
                @Override
                public void flatMap(LineString lineString, Collector<LineString> collector) throws Exception {

                    int cellIDCounter = 0;
                    for (String lineStringGridID : lineString.gridIDsSet) {
                        if (guaranteedNeighboringCells.contains(lineStringGridID)) {
                            cellIDCounter++;
                            // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                            if (cellIDCounter == lineString.gridIDsSet.size()) {
                                collector.collect(lineString);
                            }
                        } else { // candidate neighbors
                            Double distance;
                            if (approximateQuery) {
                                distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryPolygon.boundingBox, lineString.boundingBox);
                            } else {
                                distance = DistanceFunctions.getDistance(queryPolygon, lineString);
                            }

                            if (distance <= queryRadius) {
                                collector.collect(lineString);
                            }
                            break;
                        }
                    }

                }
            }).name("Real-time - LINESTRING - POLYGON");

            return rangeQueryNeighbours;
        }

        //--------------- Real-time - LINESTRING - LINESTRING -----------------//
        public static DataStream<LineString> RangeQuery
        (DataStream < LineString > lineStringStream, LineString queryLineString,double queryRadius, UniformGrid uGrid,
         boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryLineString);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryLineString, guaranteedNeighboringCells);
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(), candidateNeighboringCells.stream()).collect(Collectors.toSet());

            DataStream<LineString> filteredLineStrings = lineStringStream.flatMap(new cellBasedLineStringFlatMap(neighboringCells)).startNewChain();

            DataStream<LineString> rangeQueryNeighbours = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
                @Override
                public String getKey(LineString lineString) throws Exception {
                    return lineString.gridID;
                }
            }).flatMap(new FlatMapFunction<LineString, LineString>() {
                @Override
                public void flatMap(LineString lineString, Collector<LineString> collector) throws Exception {

                    int cellIDCounter = 0;
                    for (String lineStringGridID : lineString.gridIDsSet) {
                        if (guaranteedNeighboringCells.contains(lineStringGridID)) {
                            cellIDCounter++;
                            // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                            if (cellIDCounter == lineString.gridIDsSet.size()) {
                                collector.collect(lineString);
                            }
                        } else { // candidate neighbors
                            Double distance;
                            if (approximateQuery) {
                                distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryLineString.boundingBox, lineString.boundingBox);
                            } else {
                                distance = DistanceFunctions.getDistance(queryLineString, lineString);
                            }

                            if (distance <= queryRadius) {
                                collector.collect(lineString);
                            }
                            break;
                        }
                    }

                }
            }).name("Real-time - LINESTRING - LINESTRING");

            return rangeQueryNeighbours;
        }
        // ********* Real-time Queries End ********* //


        // ********* Window-based Queries ********* //
        //--------------- Window-based - POINT - LineString -----------------//
        public static DataStream<LineString> RangeQueryIncremental
        (DataStream < LineString > lineStringStream, Point queryPoint,double queryRadius, UniformGrid uGrid,
         int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

            Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);
            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);

            DataStream<LineString> streamWithTsAndWm =
                    lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(LineString ls) {
                            return ls.timeStampMillisec;
                        }
                    }).startNewChain();

            // Filtering out the linestrings which lie greater than queryRadius of the query point
            DataStream<LineString> filteredLineStrings = streamWithTsAndWm.flatMap(new cellBasedLineStringFlatMap(neighboringCells));

            DataStream<LineString> rangeQueryNeighbours = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
                @Override
                public String getKey(LineString lineString) throws Exception {
                    return lineString.gridID;
                }
            }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .apply(new RichWindowFunction<LineString, LineString, String, TimeWindow>() {

                        /**
                         * The ListState handle.
                         */
                        private transient ListState<LineString> queryOutputListState;

                        @Override
                        public void open(Configuration config) {
                            PojoTypeInfo<LineString> objTypeInfo = (PojoTypeInfo<LineString>) TypeInformation.of(LineString.class);

                            ListStateDescriptor<LineString> queryOutputStateDescriptor = new ListStateDescriptor<LineString>(
                                    "queryOutputStateDescriptor",// state name
                                    objTypeInfo);
                            //TypeInformation.of(new TypeHint<Point>() {})
                            this.queryOutputListState = getRuntimeContext().getListState(queryOutputStateDescriptor);
                        }

                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<LineString> objIterator, Collector<LineString> neighbors) throws Exception {

                            List<LineString> nextWindowUsefulOutputFromPastWindow = new ArrayList<>();
                            // Access the list state - past output
                            for (LineString obj : queryOutputListState.get()) {
                                neighbors.collect(obj);

                                // Storing objects useful for next window
                                if (obj.timeStampMillisec >= (timeWindow.getStart() + (slideStep * 1000))) {
                                    nextWindowUsefulOutputFromPastWindow.add(obj);
                                }
                            }

                            // Clear the list state
                            queryOutputListState.clear();
                            // Populating the list state with the objects useful for next window
                            queryOutputListState.addAll(nextWindowUsefulOutputFromPastWindow);

                            for (LineString lineString : objIterator) {
                                // Check for Range Query only for new objects
                                if (lineString.timeStampMillisec >= (timeWindow.getEnd() - (slideStep * 1000))) {

                                    int cellIDCounter = 0;
                                    for (String lineStringGridID : lineString.gridIDsSet) {
                                        if (guaranteedNeighboringCells.contains(lineStringGridID)) {
                                            cellIDCounter++;
                                            // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                            if (cellIDCounter == lineString.gridIDsSet.size()) {
                                                neighbors.collect(lineString);
                                                queryOutputListState.add(lineString); // add new output useful for next window
                                            }
                                        } else { // candidate neighbors

                                            Double distance;
                                            if (approximateQuery) {
                                                distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(queryPoint, lineString);
                                            } else {
                                                distance = DistanceFunctions.getDistance(queryPoint, lineString);
                                            }

                                            if (distance <= queryRadius) {
                                                neighbors.collect(lineString);
                                                queryOutputListState.add(lineString); // add new output useful for next window
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }).name("Window-based - POINT - LINESTRING - Incremental");

            return rangeQueryNeighbours;
        }

        //--------------- Window-based - POINT - LineString -----------------//
        public static DataStream<LineString> RangeQuery(DataStream < LineString > lineStringStream, Point
                queryPoint, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness,
                                                        boolean approximateQuery){

            Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);
            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);

            DataStream<LineString> streamWithTsAndWm =
                    lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(LineString ls) {
                            return ls.timeStampMillisec;
                        }
                    }).startNewChain();

            // Filtering out the linestrings which lie greater than queryRadius of the query point
            DataStream<LineString> filteredLineStrings = streamWithTsAndWm.flatMap(new cellBasedLineStringFlatMap(neighboringCells));

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
                                int cellIDCounter = 0;
                                for (String lineStringGridID : lineString.gridIDsSet) {
                                    if (guaranteedNeighboringCells.contains(lineStringGridID)) {
                                        cellIDCounter++;
                                        // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                        if (cellIDCounter == lineString.gridIDsSet.size()) {
                                            neighbors.collect(lineString);
                                        }
                                    } else { // candidate neighbors

                                        Double distance;
                                        if (approximateQuery) {
                                            distance = DistanceFunctions.getPointLineStringBBoxMinEuclideanDistance(queryPoint, lineString);
                                        } else {
                                            distance = DistanceFunctions.getDistance(queryPoint, lineString);
                                        }

                                        if (distance <= queryRadius) {
                                            neighbors.collect(lineString);
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }).name("Window-based - POINT - LINESTRING");

            return rangeQueryNeighbours;
        }

        //--------------- WINDOW-based - LINESTRING - POLYGON -----------------//
        public static DataStream<LineString> RangeQuery(DataStream < LineString > lineStringStream, Polygon
                queryPolygon, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep, int allowedLateness,
                                                        boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(), candidateNeighboringCells.stream()).collect(Collectors.toSet());

            DataStream<LineString> streamWithTsAndWm =
                    lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(LineString ls) {
                            return ls.timeStampMillisec;
                        }
                    }).startNewChain();

            DataStream<LineString> filteredLineStrings = streamWithTsAndWm.flatMap(new cellBasedLineStringFlatMap(neighboringCells));

            DataStream<LineString> rangeQueryNeighbours = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
                @Override
                public String getKey(LineString lineString) throws Exception {
                    return lineString.gridID;
                }
            }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .apply(new WindowFunction<LineString, LineString, String, TimeWindow>() {
                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<LineString> lineStringIterator, Collector<LineString> neighbors) throws Exception {

                            for (LineString lineString : lineStringIterator) {
                                int cellIDCounter = 0;
                                for (String lineStringGridID : lineString.gridIDsSet) {
                                    if (guaranteedNeighboringCells.contains(lineStringGridID)) {
                                        cellIDCounter++;
                                        // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                        if (cellIDCounter == lineString.gridIDsSet.size()) {
                                            neighbors.collect(lineString);
                                        }
                                    } else { // candidate neighbors
                                        Double distance;
                                        if (approximateQuery) {
                                            distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryPolygon.boundingBox, lineString.boundingBox);
                                        } else {
                                            distance = DistanceFunctions.getDistance(queryPolygon, lineString);
                                        }

                                        if (distance <= queryRadius) {
                                            neighbors.collect(lineString);
                                        }
                                        break;
                                    }
                                }
                            }

                        }
                    }).name("Window-based - LINESTRING - POLYGON");

            return rangeQueryNeighbours;
        }

        //--------------- WINDOW-based - LINESTRING - LINESTRING -----------------//
        public static DataStream<LineString> RangeQuery
        (DataStream < LineString > lineStringStream, LineString queryLineString,double queryRadius, UniformGrid uGrid,
         int windowSize, int slideStep, int allowedLateness, boolean approximateQuery){

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryLineString);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryLineString, guaranteedNeighboringCells);
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(), candidateNeighboringCells.stream()).collect(Collectors.toSet());

            DataStream<LineString> streamWithTsAndWm =
                    lineStringStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LineString>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(LineString ls) {
                            return ls.timeStampMillisec;
                        }
                    }).startNewChain();

            DataStream<LineString> filteredLineStrings = streamWithTsAndWm.flatMap(new cellBasedLineStringFlatMap(neighboringCells));

            DataStream<LineString> rangeQueryNeighbours = filteredLineStrings.keyBy(new KeySelector<LineString, String>() {
                @Override
                public String getKey(LineString lineString) throws Exception {
                    return lineString.gridID;
                }
            }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .apply(new WindowFunction<LineString, LineString, String, TimeWindow>() {
                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<LineString> lineStringIterator, Collector<LineString> neighbors) throws Exception {

                            for (LineString lineString : lineStringIterator) {
                                int cellIDCounter = 0;
                                for (String lineStringGridID : lineString.gridIDsSet) {
                                    if (guaranteedNeighboringCells.contains(lineStringGridID)) {
                                        cellIDCounter++;
                                        // If all the lineString bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                        if (cellIDCounter == lineString.gridIDsSet.size()) {
                                            neighbors.collect(lineString);
                                        }
                                    } else { // candidate neighbors
                                        Double distance;
                                        if (approximateQuery) {
                                            distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(queryLineString.boundingBox, lineString.boundingBox);
                                        } else {
                                            distance = DistanceFunctions.getDistance(lineString, queryLineString);
                                        }

                                        if (distance <= queryRadius) {
                                            neighbors.collect(lineString);
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }).name("Window-based - LINESTRING - LINESTRING");

            return rangeQueryNeighbours;
        }
        // ********* Window-based Queries End ********* //
    }

    public static class polygonTrigger extends Trigger<Polygon, TimeWindow> {

        private int slideStep;
        ValueStateDescriptor<Boolean> firstWindowDesc = new ValueStateDescriptor<Boolean>("isFirstWindow", Boolean.class);

        //ctor
        public polygonTrigger(){}
        public polygonTrigger(int slideStep){
            this.slideStep = slideStep;
        }

        @Override
        public TriggerResult onElement(Polygon polygon, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

            ValueState<Boolean> firstWindow = triggerContext.getPartitionedState(firstWindowDesc);




            //Using states manage the first window, so that all the tuples can be processed
            if(firstWindow.value() == null){

                if(true) {
                    firstWindow.update(false);
                }

                return TriggerResult.CONTINUE;



            }
            else {
                if (polygon.timeStampMillisec >= (timeWindow.getEnd() - (slideStep * 1000)))
                    return TriggerResult.CONTINUE; // Do nothing
                else
                    return TriggerResult.PURGE; // Delete
            }
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }

    // ********* Window-based Queries End ********* //

    // Misc Classes
    public static class getCellsFilteredByLayer extends RichFilterFunction<Tuple2<String, Integer>>
    {
        private final HashSet<String> CellIDs; // CellIDs are input parameters

        //ctor
        public getCellsFilteredByLayer(HashSet<String> CellIDs)
        {
            this.CellIDs = CellIDs;
        }

        @Override
        public boolean filter(Tuple2<String, Integer> cellIDCount) throws Exception
        {
            return CellIDs.contains(cellIDCount.f0);
        }
    }


    public static class cellBasedLineStringFlatMap implements FlatMapFunction<LineString, LineString>{

        Set<String> neighboringCells = new HashSet<String>();

        //ctor
        public cellBasedLineStringFlatMap() {}
        public cellBasedLineStringFlatMap(Set<String> neighboringCells) {
            this.neighboringCells = neighboringCells;
        }

        @Override
        public void flatMap(LineString lineString, Collector<LineString> output) throws Exception {

            // If a polygon is either a CN or GN
            LineString outputLineString;
            for(String gridID: lineString.gridIDsSet) {
                if (neighboringCells.contains(gridID)) {
                    outputLineString = new LineString(lineString.objID, Arrays.asList(lineString.lineString.getCoordinates()), lineString.gridIDsSet, gridID, lineString.boundingBox);
                    output.collect(outputLineString);
                    return;
                }
            }
        }
    }
}

