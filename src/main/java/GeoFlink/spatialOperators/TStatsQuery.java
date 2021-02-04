package GeoFlink.spatialOperators;

import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.Set;

public class TStatsQuery implements Serializable {

    //--------------- TStatsQuery QUERY - Real-time -----------------//
    //public static DataStream<Tuple4<String, Double, Long, Double>> TSpatialStatsQuery(DataStream<Point> pointStream, Set<String> trajIDSet){
    public static DataStream<Tuple5<String, Double, Long, Double, Long>> TSpatialStatsQuery(DataStream<Point> pointStream, Set<String> trajIDSet){

        DataStream<Point> filteredStream = pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {

                if (trajIDSet.size() > 0)
                    return ((trajIDSet.contains(point.objID)));
                else
                    return true;
            }
        });

        return filteredStream.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
        }).flatMap(new TStatsQueryFlatmapFunction());
    }


    //--------------- TStatsQuery QUERY - Window-based -----------------//
    public static DataStream<Tuple4<String, Double, Long, Double>> TSpatialStatsQuery(DataStream<Point> pointStream, Set<String> trajIDSet, int windowSize, int windowSlideStep){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Point> filteredStream = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                if (trajIDSet.size() > 0)
                    return ((trajIDSet.contains(point.objID)));
                else
                    return true;
            }
        });

        return filteredStream.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
        }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new TStatsQueryWFunction());
    }


    // User Defined Classes
    // Update/Output is generated only when the current tuple timestamp is different from previous tuple
    //public static class TStatsQueryFlatmapFunction extends RichFlatMapFunction<Point, Tuple4<String, Double, Long, Double>> {
    public static class TStatsQueryFlatmapFunction extends RichFlatMapFunction<Point, Tuple5<String, Double, Long, Double, Long>> {

        private ValueState<Long> temporalLengthVState;
        private ValueState<Long> lastTimestampVState;
        private ValueState<Double> spatialLengthVState;
        private ValueState<Double> lastPointCoordinateXVState;
        private ValueState<Double> lastPointCoordinateYVState;

        //ctor
        public  TStatsQueryFlatmapFunction() {}

        @Override
        public void open(Configuration config) {

            ValueStateDescriptor<Long> temporalLengthDescriptor = new ValueStateDescriptor<Long>(
                    "temporalLengthDescriptor", // state name
                    BasicTypeInfo.LONG_TYPE_INFO);

            ValueStateDescriptor<Long> lastTimestampDescriptor = new ValueStateDescriptor<Long>(
                    "lastTimestampDescriptor", // state name
                    BasicTypeInfo.LONG_TYPE_INFO);

            ValueStateDescriptor<Double> spatialLengthDescriptor = new ValueStateDescriptor<Double>(
                    "spatialLengthDescriptor", // state name
                    BasicTypeInfo.DOUBLE_TYPE_INFO);

            ValueStateDescriptor<Double> lastPointCoordinateXDescriptor = new ValueStateDescriptor<Double>(
                    "lastPointCoordinateXDescriptor", // state name
                    BasicTypeInfo.DOUBLE_TYPE_INFO);

            ValueStateDescriptor<Double> lastPointCoordinateYDescriptor = new ValueStateDescriptor<Double>(
                    "lastPointCoordinateYDescriptor", // state name
                    BasicTypeInfo.DOUBLE_TYPE_INFO);

            this.temporalLengthVState = getRuntimeContext().getState(temporalLengthDescriptor);
            this.lastTimestampVState = getRuntimeContext().getState(lastTimestampDescriptor);
            this.spatialLengthVState = getRuntimeContext().getState(spatialLengthDescriptor);
            this.lastPointCoordinateXVState = getRuntimeContext().getState(lastPointCoordinateXDescriptor);
            this.lastPointCoordinateYVState = getRuntimeContext().getState(lastPointCoordinateYDescriptor);
        }

        //Tuple4.of(p.objID, temporalLength, spatialLength, spatialLength/temporalLength);
        @Override
        //public void flatMap(Point p, Collector<Tuple4<String, Double, Long, Double>> out) throws Exception {
        public void flatMap(Point p, Collector<Tuple5<String, Double, Long, Double, Long>> out) throws Exception {
            Double spatialLength;
            Long temporalLength;
            Long lastTimestamp;
            Double lastPointCoordinateX;
            Double lastPointCoordinateY;

            // Fetching the value of state variables
            spatialLength = spatialLengthVState.value();
            temporalLength = temporalLengthVState.value();
            lastTimestamp = lastTimestampVState.value();
            lastPointCoordinateX = lastPointCoordinateXVState.value();
            lastPointCoordinateY = lastPointCoordinateYVState.value();

            // If this is the first point, i.e., no past value is available
            if (lastTimestamp == null){
                lastTimestamp = p.timeStampMillisec;
                lastPointCoordinateX = p.point.getX();
                lastPointCoordinateY = p.point.getY();
                temporalLength = 0L;
                spatialLength = 0.0;

                // Updating the state variables
                temporalLengthVState.update(temporalLength);
                lastTimestampVState.update(lastTimestamp);
                spatialLengthVState.update(spatialLength);
                lastPointCoordinateXVState.update(lastPointCoordinateX);
                lastPointCoordinateYVState.update(lastPointCoordinateY);

            }else {
                if (p.timeStampMillisec > lastTimestamp) // Avoiding out-of-order arrival of tuples
                {
                    Date date = new Date();
                    //Double currSpatialDist = HelperClass.computeHaverSine(lastPointCoordinateX, lastPointCoordinateY, p.point.getX(), p.point.getY());
                    Double currSpatialDist = HelperClass.getPointPointEuclideanDistance(lastPointCoordinateX, lastPointCoordinateY, p.point.getX(), p.point.getY());
                    Long currTemporalDist = p.timeStampMillisec - lastTimestamp;

                    spatialLength += currSpatialDist;
                    temporalLength += currTemporalDist;

                    lastTimestamp = p.timeStampMillisec;
                    lastPointCoordinateX = p.point.getX();
                    lastPointCoordinateY = p.point.getY();

                    // Updating the state variables
                    temporalLengthVState.update(temporalLength);
                    lastTimestampVState.update(lastTimestamp);
                    spatialLengthVState.update(spatialLength);
                    lastPointCoordinateXVState.update(lastPointCoordinateX);
                    lastPointCoordinateYVState.update(lastPointCoordinateY);

                    //out.collect(Tuple4.of(p.objID, spatialLength, temporalLength, spatialLength/temporalLength));
                    //System.out.println(date.getTime() - p.ingestionTime);
                    out.collect(Tuple5.of(p.objID, spatialLength, temporalLength, spatialLength/temporalLength, (date.getTime() - p.ingestionTime)));
                }
            }
        }
    }


    //RichWindowFunction<IN, OUT, KEY, W>
    public static class TStatsQueryWFunction extends RichWindowFunction<Point, Tuple4<String, Double, Long, Double>, String, TimeWindow> {

        //ctor
        public  TStatsQueryWFunction() {};

        @Override
        public void apply(String key, TimeWindow window, Iterable<Point> input, Collector<Tuple4<String, Double, Long, Double>> output) throws Exception {

            Long temporalLength = 0L;
            Long lastTimestamp = 0L;
            Double spatialLength = 0.0;
            Double lastPointCoordinateX = 0.0;
            Double lastPointCoordinateY = 0.0;

            // Check all points in the window
            for (Point p : input) {
                if (lastTimestamp.equals(0L)){ // case of first point p in the loop
                    lastTimestamp = p.timeStampMillisec;
                    lastPointCoordinateX = p.point.getX();
                    lastPointCoordinateY = p.point.getY();
                    spatialLength = 0.0;
                    temporalLength = 0L;
                }else {
                    if (p.timeStampMillisec > lastTimestamp) // Avoiding out-of-order arrival of tuples
                    {
                        //Double currSpatialDist = HelperClass.computeHaverSine(lastPointCoordinateX, lastPointCoordinateY, p.point.getX(), p.point.getY());
                        Double currSpatialDist = HelperClass.getPointPointEuclideanDistance(lastPointCoordinateX, lastPointCoordinateY, p.point.getX(), p.point.getY());
                        Long currTemporalDist = p.timeStampMillisec - lastTimestamp;

                        spatialLength += currSpatialDist;
                        temporalLength += currTemporalDist;

                        lastTimestamp = p.timeStampMillisec;
                        lastPointCoordinateX = p.point.getX();
                        lastPointCoordinateY = p.point.getY();
                    }
                }
            }
            output.collect(Tuple4.of(key,  spatialLength, temporalLength, spatialLength/temporalLength));
        }
    }
}
