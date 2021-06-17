package GeoFlink.spatialOperators.tAggregate;

import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;


public class PointTAggregateQuery extends TAggregateQuery<Point> {
    public PointTAggregateQuery(QueryConfiguration conf) {
        super.initializeKNNQuery(conf);
    }

    public DataStream<?> run(DataStream<Point> pointStream, String aggregateFunction, String windowType, Long inactiveTrajDeletionThreshold) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        //--------------- Real-time - POINT -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            return realTime(pointStream, aggregateFunction, inactiveTrajDeletionThreshold);
        }

        //--------------- Window-based - POINT -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int windowSlideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(pointStream, aggregateFunction, windowType, windowSize, windowSlideStep);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple4<String, Integer, HashMap<String, Long>, Long>> realTime(DataStream<GeoFlink.spatialObjects.Point> pointStream, String aggregateFunction, Long inactiveTrajDeletionThreshold) {

        // Filtering out the cells which do not fall into the grid cells
        DataStream<GeoFlink.spatialObjects.Point> spatialStreamWithoutNullCellID = pointStream.filter(new FilterFunction<GeoFlink.spatialObjects.Point>() {
            @Override
            public boolean filter(GeoFlink.spatialObjects.Point p) throws Exception {
                return (p.gridID != null);
            }
        });

        //DataStream<Tuple3<String, Integer, HashMap<String, Long>>> cWindowedCellBasedStayTime = spatialStreamWithoutNullCellID
        DataStream<Tuple4<String, Integer, HashMap<String, Long>, Long>> cWindowedCellBasedStayTime = spatialStreamWithoutNullCellID
                .keyBy(new gridCellKeySelector())
                .map(new THeatmapAggregateQueryMapFunction(aggregateFunction, inactiveTrajDeletionThreshold));

        return cWindowedCellBasedStayTime;
    }

    // WINDOW BASED
    private DataStream<Tuple5<String, Integer, Long, Long, HashMap<String, Long>>> windowBased(DataStream<Point> pointStream, String aggregateFunction, String windowType, long windowSize, long windowSlideStep) {

        // Filtering out the cells which do not fall into the grid cells
        DataStream<Point> spatialStreamWithoutNullCellID = pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point p) throws Exception {
                return (p.gridID != null);
            }
        });

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> spatialStreamWithTsAndWm =
                spatialStreamWithoutNullCellID.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        if(windowType.equalsIgnoreCase("COUNT")){

            DataStream<Tuple5<String, Integer, Long, Long, HashMap<String, Long>>> cWindowedCellBasedStayTime = spatialStreamWithTsAndWm
                    .keyBy(new gridCellKeySelector())
                    .countWindow(windowSize, windowSlideStep)
                    .process(new CountWindowProcessFunction(aggregateFunction)).name("Count Window");

            return cWindowedCellBasedStayTime;

        }
        else { // Default TIME Window

            DataStream<Tuple5<String, Integer, Long, Long, HashMap<String, Long>>> tWindowedCellBasedStayTime = spatialStreamWithTsAndWm
                    .keyBy(new gridCellKeySelector())
                    //.window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                    .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                    .process(new TimeWindowProcessFunction(aggregateFunction)).name("Time Window");

            return tWindowedCellBasedStayTime;
        }
    }
}
