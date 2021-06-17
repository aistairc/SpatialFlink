package GeoFlink.spatialOperators.range;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
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

import java.util.Set;

public class PointLineStringRangeQuery extends RangeQuery<Point, LineString> {
    public PointLineStringRangeQuery(QueryConfiguration conf, SpatialIndex index) {
        super.initializeRangeQuery(conf, index);
    }

    public DataStream<Point> run(DataStream<Point> pointStream, LineString queryLineString, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();
        //--------------- Real-time - LINESTRING - POINT -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
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
        //--------------- Window-based - LINESTRING - POINT -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();

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
        } else {
            throw new IllegalArgumentException("Not yet support");
        }
    }
}
