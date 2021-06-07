package GeoFlink.spatialOperators.range;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.DistanceFunctions;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PointPointRangeQuery extends RangeQuery<Point, Point> {
    //--------------- Real-time - POINT - POINT -----------------//
    public PointPointRangeQuery(QueryConfiguration conf, SpatialIndex index){
        super.initializeRangeQuery(conf, index);
    }

    public DataStream<Point> run(DataStream<Point> pointStream, Point queryPoint, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

        //--------------- Real-time - POINT - POINT -----------------//
        if(this.getQueryConfiguration().getQueryType() == QueryType.RealTime)
        {
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

        //--------------- Window-based - POINT - POINT -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased)
        {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();

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

            //--------------- Window-based - POINT - POINT -----------------//
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

                                    if(approximateQuery) { // all the candidate neighbors are sent to output
                                        neighbors.collect(point);
                                    }else{
                                        Double distance = DistanceFunctions.getDistance(queryPoint, point);

                                        if (distance <= queryRadius){
                                            neighbors.collect(point);
                                        }
                                    }
                                }
                            }
                        }
                    }).name("Window-based - POINT - POINT");

            return rangeQueryNeighbours;
        }else{
            throw new IllegalArgumentException("Not yet support");
        }
    }

    //--------------- Window-based - POINT - POINT -----------------//
    public DataStream<Point> queryIncremental(DataStream<Point> pointStream, Point queryPoint, double queryRadius){
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();
        int windowSize = this.getQueryConfiguration().getWindowSize();
        int slideStep = this.getQueryConfiguration().getSlideStep();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

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
                        for(Point point:queryOutputListState.get()){
                            //System.out.println("state " + point);
                            neighbors.collect(point);

                            // Storing points useful for next window
                            if(point.timeStampMillisec >= (timeWindow.getStart() + (slideStep * 1000))) {
                                nextWindowUsefulOutputFromPastWindow.add(point);
                            }
                        }

                        // Clear the list state
                        queryOutputListState.clear();
                        // Populating the list state with the points useful for next window
                        queryOutputListState.addAll(nextWindowUsefulOutputFromPastWindow);

                        for (Point point : pointIterator) {
                            // Check for Range Query only for new objects
                            if(point.timeStampMillisec >= (timeWindow.getEnd() - (slideStep * 1000))) {

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
}
