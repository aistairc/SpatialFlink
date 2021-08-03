package GeoFlink.spatialOperators.range;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class PointPolygonRangeQuery extends RangeQuery<Point, Polygon> {
    public PointPolygonRangeQuery(QueryConfiguration conf, SpatialIndex index) {
        super.initializeRangeQuery(conf, index);
    }

    public DataStream<Point> run(DataStream<Point> pointStream, Set<Polygon> queryPolygonSet, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();
        //--------------- Real-time - POLYGON - POINT -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {

            HashSet<String> candidateNeighboringCells = new HashSet<>();
            HashSet<String> guaranteedNeighboringCells = new HashSet<>();

            for(Polygon polygon:queryPolygonSet) {
                //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((Polygon[]) queryPolygonSet.toArray())[0]);
                //Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, ((Polygon[]) queryPolygonSet.toArray())[0], guaranteedNeighboringCells);
                guaranteedNeighboringCells.addAll(uGrid.getGuaranteedNeighboringCells(queryRadius, polygon));
                candidateNeighboringCells.addAll(uGrid.getCandidateNeighboringCells(queryRadius, polygon, guaranteedNeighboringCells));
                //candidateNeighboringCells.addAll(guaranteedNeighboringCells);
            }

            DataStream<Point> filteredPoints = pointStream.filter(new FilterFunction<Point>() {
                @Override
                public boolean filter(Point point) throws Exception {
                    return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
                }
            }).startNewChain();


            return filteredPoints.keyBy(new KeySelector<Point, String>() {
                @Override
                public String getKey(Point p) throws Exception {
                    return p.gridID;
                }
            }).flatMap(new FlatMapFunction<Point, Point>() {
                @Override
                public void flatMap(Point point, Collector<Point> collector) throws Exception {

                    if (guaranteedNeighboringCells.contains(point.gridID)) {
                        collector.collect(point);
                    }
                    else {

                        double distance;

                        for(Polygon polygon:queryPolygonSet) {

                            if (approximateQuery) {
                                distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, polygon);
                            } else {
                                distance = DistanceFunctions.getDistance(point, polygon);
                            }
                            if (distance <= queryRadius) {
                                collector.collect(point);
                                break;
                            }

                            /*
                            if (approximateQuery) {
                                distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, ((Polygon[]) queryPolygonSet.toArray())[0]);
                            } else {
                                distance = DistanceFunctions.getDistance(point, ((Polygon[]) queryPolygonSet.toArray())[0]);
                            }
                            if (distance <= queryRadius) {
                                collector.collect(point);
                            }
                             */
                        }
                    }

                }
            }).name("Real-time - POLYGON - POINT");
        }

        //--------------- Real-time Naive - POLYGON - POINT -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.RealTimeNaive) {

            return pointStream.keyBy(new KeySelector<Point, String>() {
                @Override
                public String getKey(Point p) throws Exception {
                    return p.objID;
                }
            }).flatMap(new FlatMapFunction<Point, Point>() {
                @Override
                public void flatMap(Point point, Collector<Point> collector) throws Exception {

                        double distance;
                        for(Polygon polygon:queryPolygonSet) {

                            if (approximateQuery) {
                                distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, polygon);
                            } else {
                                distance = DistanceFunctions.getDistance(point, polygon);
                            }

                            if (distance <= queryRadius) {
                                collector.collect(point);
                                break;
                            }
                    }
                }
            }).name("Real-time - POLYGON - POINT");
        }

        //--------------- Window-based - POLYGON - POINT -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();

            HashSet<String> candidateNeighboringCells = new HashSet<>();
            HashSet<String> guaranteedNeighboringCells = new HashSet<>();

            for(Polygon polygon:queryPolygonSet) {
                //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((Polygon[]) queryPolygonSet.toArray())[0]);
                //Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, ((Polygon[]) queryPolygonSet.toArray())[0], guaranteedNeighboringCells);
                guaranteedNeighboringCells.addAll(uGrid.getGuaranteedNeighboringCells(queryRadius, polygon));
                candidateNeighboringCells.addAll(uGrid.getCandidateNeighboringCells(queryRadius, polygon, guaranteedNeighboringCells));
                //candidateNeighboringCells.addAll(guaranteedNeighboringCells);
            }

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

            return filteredPoints.keyBy(new KeySelector<Point, String>() {
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

                                    double distance;
                                    for(Polygon polygon:queryPolygonSet) {

                                        if (approximateQuery) {
                                            distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, polygon);
                                        } else {
                                            distance = DistanceFunctions.getDistance(point, polygon);
                                        }
                                        if (distance <= queryRadius) {
                                            neighbors.collect(point);
                                            break;
                                        }

                            /*
                            if (approximateQuery) {
                                distance = DistanceFunctions.getPointPolygonBBoxMinEuclideanDistance(point, ((Polygon[]) queryPolygonSet.toArray())[0]);
                            } else {
                                distance = DistanceFunctions.getDistance(point, ((Polygon[]) queryPolygonSet.toArray())[0]);
                            }
                            if (distance <= queryRadius) {
                                collector.collect(point);
                            }
                             */
                                    }
                                }
                            }
                        }
                    }).name("Window-based - POLYGON - POINT");
        } else {
            throw new IllegalArgumentException("Not yet support");
        }
    }



    public DataStream<Long> queryLatency(DataStream<Point> pointStream, Polygon queryPolygon, double queryRadius){
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();
        //--------------- Real-time - POLYGON - POINT -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {

            Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPolygon);
            Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPolygon, guaranteedNeighboringCells);

            final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

            DataStream<Point> filteredPoints = pointStream.filter(new FilterFunction<Point>() {
                @Override
                public boolean filter(Point point) throws Exception {
                    return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
                }
            }).startNewChain();

            return filteredPoints.keyBy(new KeySelector<Point, String>() {
                @Override
                public String getKey(Point p) throws Exception {
                    return p.gridID;
                }
            }).flatMap(new FlatMapFunction<Point, Long>() {
                @Override
                public void flatMap(Point point, Collector<Long> collector) throws Exception {

                    if (guaranteedNeighboringCells.contains(point.gridID)) {
                        Date date = new Date();
                        Long latency = date.getTime() -  point.timeStampMillisec;
                        collector.collect(latency);
                    }
                    else {
                        Date date = new Date();
                        Long latency = date.getTime() -  point.timeStampMillisec;
                        collector.collect(latency);
                    }

                }
            }).name("Real-time - POLYGON - POINT");
        }
        //--------------- Window-based - POLYGON - POINT -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();

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
                public boolean filter(Point point) {
                    return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
                }
            });

            return filteredPoints.keyBy(new KeySelector<Point, String>() {
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
                                    Long latency = date.getTime() -  point.timeStampMillisec;
                                    neighbors.collect(latency);
                                }
                                else {
                                    Date date = new Date();
                                    Long latency = date.getTime() -  point.timeStampMillisec;
                                    neighbors.collect(latency);
                                }
                            }
                        }
                    }).name("Window-based - POLYGON - POINT");
        } else {
            throw new IllegalArgumentException("Not yet supported");
        }
    }
}
