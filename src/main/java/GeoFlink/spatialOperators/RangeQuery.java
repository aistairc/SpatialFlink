package GeoFlink.spatialOperators;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Set;

public class RangeQuery implements Serializable {

    //--------------- GRID-BASED RANGE QUERY -----------------//
    public static DataStream<Point> SpatialRangeQuery(DataStream<Point> pointStream, Point queryPoint, double queryRadius, int windowSize, int slideStep, UniformGrid uGrid) {

        Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint);
        Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint, guaranteedNeighboringCells);

        DataStream<Point> filteredPoints = pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                return ((candidateNeighboringCells.contains(point.gridID)) || (guaranteedNeighboringCells.contains(point.gridID)));
            }
        });

        DataStream<Point> unaggregatedNeighbours = filteredPoints.keyBy(new KeySelector<Point, String>() {
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
                                Double distance = HelperClass.computeEuclideanDistance(queryPoint.point.getX(), queryPoint.point.getY(), point.point.getX(),point.point.getY());
                                if (distance <= queryRadius)
                                { neighbors.collect(point);}
                            }
                        }
                    }
                }).name("Windowed (Apply) Grid Based");

        DataStream<Point> AggregatedNeighbours = unaggregatedNeighbours.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new AllWindowFunction<Point, Point, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Point> pointIterator, Collector<Point> neighbors) throws Exception {
                        for (Point point : pointIterator) {
                            neighbors.collect(point);
                        }
                    }
                });

        return AggregatedNeighbours;
    }
}
