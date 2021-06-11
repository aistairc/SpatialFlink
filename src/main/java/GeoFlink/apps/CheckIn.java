package GeoFlink.apps;

import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

public class CheckIn implements Serializable {

    //--------------- CheckIn QUERY for DEIM -----------------//
    public static DataStream<Tuple4<String, Integer, Integer, Long>> CheckInQuery(DataStream<Point> pointStream, HashMap<String, Integer> roomCapacities, int windowSize){

        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(100)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });


        DataStream<Point> streamWInsertedMissingValues = pointStreamWithTsAndWm
                .keyBy(p -> p.userID)
                .countWindow(2, 1)
                //ProcessWindowFunction<IN, OUT, KEY, W extends Window>
                .process(new ProcessWinForInsertingMissingValues());

        //streamWInsertedMissingValues.print();

        return streamWInsertedMissingValues
                .keyBy(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        //System.out.println(p.deviceID.substring(0, p.deviceID.indexOf("-")));
                        return p.deviceID.substring(0, p.deviceID.indexOf("-"));
                    }
                })
                .countWindow(1)
                .process(new ProcessForCountingObjects(roomCapacities));



        /*
        DataStream<Point> streamWInsertedMissingValues = pointStreamWithTsAndWm
                .keyBy(p -> p.userID)
                .window(TumblingEventTimeWindows.of(Time.hours(windowSize)))
                .trigger(new onElementTrigger())
                //.trigger(CountTrigger.of(1))
                .process(new ProcessWindowFunction<Point, Point, String, TimeWindow>() {

                    @Override
                    public void process(String deviceID, Context timeWindow, Iterable<Point> pointIterator, Collector<Point> output) throws Exception {

                        //Integer roomCapacity = roomCapacities.get(deviceID);
                        Map<Long, Point> pointsOrderedByTimestamp = new TreeMap<>();
                        for (Point p : pointIterator) {
                            pointsOrderedByTimestamp.put(p.timeStampMillisec, p);
                        }

                        String pastDeviceID = "";
                        int idx = -1;
                        String pastDeviceIDName = "";
                        String pastDeviceIDSymbol = "";
                        long pastTimestamp = 0L;

                        for (Map.Entry<Long, Point> entry : pointsOrderedByTimestamp.entrySet()) {

                            // If the past device ID is same as that of current, i.e. 2 consecutive ins or outs, add a new between tuple
                            if (entry.getValue().deviceID.equals(pastDeviceID)) {
                                Point p;
                                String newDeviceID = "";
                                Long newTimestamp = (pastTimestamp + entry.getValue().timeStampMillisec)/2;
                                idx = pastDeviceID.indexOf("-");
                                pastDeviceIDName = pastDeviceID.substring(0,  idx);
                                pastDeviceIDSymbol = pastDeviceID.substring(idx + 1);

                                if(pastDeviceIDSymbol.equals("in")) {
                                    newDeviceID = pastDeviceIDName.concat("-out");
                                    p = new Point(entry.getValue().eventID, newDeviceID, entry.getValue().userID, newTimestamp, entry.getValue().point.getX(), entry.getValue().point.getY());
                                }
                                else {
                                    newDeviceID = pastDeviceIDName.concat("-in");
                                    p = new Point(entry.getValue().eventID, newDeviceID, entry.getValue().userID, newTimestamp, entry.getValue().point.getX(), entry.getValue().point.getY());
                                }

                                pastDeviceID = entry.getValue().deviceID;
                                pastTimestamp = entry.getValue().timeStampMillisec;
                                output.collect(p);
                                output.collect(entry.getValue());

                            } else { // If the past device ID is different from current
                                pastDeviceID = entry.getValue().deviceID;
                                pastTimestamp = entry.getValue().timeStampMillisec;
                                output.collect(entry.getValue());
                            }
                        }
                    }
                });
         */

        /*
        DataStream<Point> streamWInsertedMissingValues = pointStreamWithTsAndWm
                .keyBy(p -> p.userID)
                .countWindow(10000, 1)
                //ProcessWindowFunction<IN, OUT, KEY, W extends Window>
                .process(new ProcessWindowFunction<Point, Point, String, GlobalWindow>() {

                    // KEY key, Context context, Iterable<IN> elements, Collector<OUT> out
                    @Override
                    public void process(String deviceID, Context countWindow, Iterable<Point> pointIterator, Collector<Point> output) throws Exception {

                        //Integer roomCapacity = roomCapacities.get(deviceID);
                        Map<Long, Point> pointsOrderedByTimestamp = new TreeMap<>();
                        for (Point p : pointIterator) {
                            pointsOrderedByTimestamp.put(p.timeStampMillisec, p);
                        }

                        String pastDeviceID = "";
                        int idx = -1;
                        String pastDeviceIDName = "";
                        String pastDeviceIDSymbol = "";
                        long pastTimestamp = 0L;

                        for (Map.Entry<Long, Point> entry : pointsOrderedByTimestamp.entrySet()) {

                            // If the past device ID is same as that of current, i.e. 2 consecutive ins or outs, add a new between tuple
                            if (entry.getValue().deviceID.equals(pastDeviceID)) {
                                Point p;
                                String newDeviceID = "";
                                Long newTimestamp = (pastTimestamp + entry.getValue().timeStampMillisec)/2;
                                idx = pastDeviceID.indexOf("-");
                                pastDeviceIDName = pastDeviceID.substring(0,  idx);
                                pastDeviceIDSymbol = pastDeviceID.substring(idx + 1);

                                if(pastDeviceIDSymbol.equals("in")) {
                                    newDeviceID = pastDeviceIDName.concat("-out");
                                    p = new Point(entry.getValue().eventID, newDeviceID, entry.getValue().userID, newTimestamp, entry.getValue().point.getX(), entry.getValue().point.getY());
                                }
                                else {
                                    newDeviceID = pastDeviceIDName.concat("-in");
                                    p = new Point(entry.getValue().eventID, newDeviceID, entry.getValue().userID, newTimestamp, entry.getValue().point.getX(), entry.getValue().point.getY());
                                }

                                pastDeviceID = entry.getValue().deviceID;
                                pastTimestamp = entry.getValue().timeStampMillisec;
                                output.collect(p);
                                output.collect(entry.getValue());

                            } else { // If the past device ID is different from current
                                pastDeviceID = entry.getValue().deviceID;
                                pastTimestamp = entry.getValue().timeStampMillisec;
                                output.collect(entry.getValue());
                            }
                        }
                    }
                });

        streamWInsertedMissingValues.print();

        return streamWInsertedMissingValues
                .keyBy(new KeySelector<Point, String>() {
                        @Override
                        public String getKey(Point p) throws Exception {
                            //System.out.println(p.deviceID.substring(0, p.deviceID.indexOf("-")));
                            return p.deviceID.substring(0, p.deviceID.indexOf("-"));
                        }
                    })
                .window(TumblingEventTimeWindows.of(Time.hours(windowSize)))
                .trigger(new onElementTrigger())
                .process(new ProcessWindowFunction<Point, Tuple4<String, Integer, Integer, Long>, String, TimeWindow>() {

                             @Override
                             public void process(String deviceID, Context timeWindow, Iterable<Point> pointIterator, Collector<Tuple4<String, Integer, Integer, Long>> output) throws Exception {

                                 Integer roomCapacity = roomCapacities.get(deviceID);
                                 int counter = 0;
                                 for (Point p : pointIterator) {
                                     int idx = p.deviceID.indexOf("-");
                                     if(p.deviceID.substring(idx + 1).equals("in"))
                                         counter++;
                                     else
                                         counter--;
                                 }

                                 output.collect(Tuple4.of(deviceID, roomCapacity, counter, System.currentTimeMillis()));

                             }
                         });

         */
    }

    public static class ProcessForCountingObjects extends ProcessWindowFunction<Point, Tuple4<String, Integer, Integer, Long>, String, GlobalWindow> {

        private HashMap<String, Integer> roomCapacities;
        //ctor
        public ProcessForCountingObjects() {
        }

        //ctor
        public ProcessForCountingObjects(HashMap<String, Integer> roomCapacities) {
            this.roomCapacities = roomCapacities;
        }

        private ValueState<Integer> objCounter;
        @Override
        public void open(Configuration config) {

            ValueStateDescriptor<Integer> objCounterDescriptor = new ValueStateDescriptor<Integer>(
                    "objCounterDescriptor", // state name
                    BasicTypeInfo.INT_TYPE_INFO, 0);

            this.objCounter = getRuntimeContext().getState(objCounterDescriptor);
        }

        @Override
        public void process(String deviceID, Context context, Iterable<Point> pointIterator, Collector<Tuple4<String, Integer, Integer, Long>> output) throws Exception {

            Integer roomCapacity = roomCapacities.get(deviceID);
            int counter = objCounter.value();

            for (Point p : pointIterator) {
                int idx = p.deviceID.indexOf("-");
                if(p.deviceID.substring(idx + 1).equals("in"))
                    counter++;
                else
                    counter--;
            }

            objCounter.update(counter);

            output.collect(Tuple4.of(deviceID, roomCapacity, counter, System.currentTimeMillis()));
        }
    }

    public static class ProcessWinForInsertingMissingValues extends ProcessWindowFunction<Point, Point, String, GlobalWindow> {

        //ctor
        public  ProcessWinForInsertingMissingValues() {}

        @Override
        public void process(String userID, Context countWindow, Iterable<Point> pointIterator, Collector<Point> output) throws Exception {

            Long oldTupleTimestamp = 0L;

            //To guarantee the arrival order of tuples
            TreeMap<Long, Point> pointsOrderedByTimestamp = new TreeMap<>();
            for (Point p : pointIterator) {
                pointsOrderedByTimestamp.put(p.timeStampMillisec, p);
            }

            if(!pointsOrderedByTimestamp.isEmpty()){
                oldTupleTimestamp = pointsOrderedByTimestamp.firstKey();
            }

            // If this is the first window
            if(pointsOrderedByTimestamp.size() == 1){
                output.collect(pointsOrderedByTimestamp.get(pointsOrderedByTimestamp.firstKey()));
                //System.out.println("abc " + pointsOrderedByTimestamp.get(pointsOrderedByTimestamp.firstKey()));
            }

            String pastDeviceID = "";
            int idx = -1;
            String pastDeviceIDName = "";
            String pastDeviceIDSymbol = "";
            long pastTimestamp = 0L;

            for (Map.Entry<Long, Point> entry : pointsOrderedByTimestamp.entrySet()) {

                // If the past device ID is same as that of current, i.e. 2 consecutive ins or outs, add a new between tuple
                if (entry.getValue().deviceID.equals(pastDeviceID)) {
                    Point p;
                    String newDeviceID = "";
                    Long newTimestamp = (pastTimestamp + entry.getValue().timeStampMillisec) / 2;
                    idx = pastDeviceID.indexOf("-");
                    pastDeviceIDName = pastDeviceID.substring(0, idx);
                    pastDeviceIDSymbol = pastDeviceID.substring(idx + 1);

                    if (pastDeviceIDSymbol.equals("in")) {
                        newDeviceID = pastDeviceIDName.concat("-out");
                        p = new Point(entry.getValue().eventID, newDeviceID, userID, newTimestamp, entry.getValue().point.getX(), entry.getValue().point.getY());
                    } else {
                        newDeviceID = entry.getValue().deviceID;
                        int idx2 = newDeviceID.indexOf("-");
                        String newDeviceIDName = newDeviceID.substring(0, idx2);
                        newDeviceID = newDeviceIDName.concat("-in");
                        p = new Point(entry.getValue().eventID, newDeviceID, userID, newTimestamp, entry.getValue().point.getX(), entry.getValue().point.getY());
                    }

                    pastDeviceID = entry.getValue().deviceID;
                    pastTimestamp = entry.getValue().timeStampMillisec;
                    output.collect(p);
                    output.collect(entry.getValue());

                } else { // If the past device ID is different from current
                    pastDeviceID = entry.getValue().deviceID;
                    pastTimestamp = entry.getValue().timeStampMillisec;

                    if (entry.getValue().timeStampMillisec > oldTupleTimestamp) {
                        output.collect(entry.getValue());
                    }
                }
            }
        }
    }


    public static class onElementTrigger extends Trigger{

        @Override
        public TriggerResult onElement(Object element, long timestamp, Window window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE; // We don't need onProcessingTime trigger
        }

        @Override
        public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE; // We don't need onEventTime trigger
        }

        @Override
        public void clear(Window window, TriggerContext ctx) throws Exception {

        }
    }
}

