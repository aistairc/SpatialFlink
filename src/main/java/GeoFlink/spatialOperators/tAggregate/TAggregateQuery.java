package GeoFlink.spatialOperators.tAggregate;

import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.SpatialOperator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class TAggregateQuery<T extends SpatialObject> extends SpatialOperator implements Serializable {
    private QueryConfiguration queryConfiguration;

    public QueryConfiguration getQueryConfiguration() {
        return queryConfiguration;
    }

    public void setQueryConfiguration(QueryConfiguration queryConfiguration) {
        this.queryConfiguration = queryConfiguration;
    }

    public void initializeKNNQuery(QueryConfiguration conf){
        this.setQueryConfiguration(conf);
    }

    public abstract DataStream<?> run(DataStream<T> stream, String aggregateFunction, String windowType, Long inactiveTrajDeletionThreshold);

    // User Defined Classes
    // Key selector
    protected class gridCellKeySelector implements KeySelector<Point,String> {
        @Override
        public String getKey(Point p) throws Exception {
            return p.gridID; // grid-cell id
        }
    }

    //public static class THeatmapAggregateQueryMapFunction extends RichMapFunction<Point, Tuple3<String, Integer, HashMap<String, Long>>> {
    protected class THeatmapAggregateQueryMapFunction extends RichMapFunction<Point, Tuple4<String, Integer, HashMap<String, Long>, Long>> {

        private MapState<String, Long> minTimestampTrackerIDMapState;
        private MapState<String, Long> maxTimestampTrackerIDMapState;

        //ctor
        public  THeatmapAggregateQueryMapFunction() {};

        String aggregateFunction;
        Long inactiveTrajDeletionThreshold;
        public THeatmapAggregateQueryMapFunction(String aggregateFunction, Long inactiveTrajDeletionThreshold){
            this.aggregateFunction = aggregateFunction;
            this.inactiveTrajDeletionThreshold = inactiveTrajDeletionThreshold;
        }

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<String, Long> minTimestampTrackerIDDescriptor = new MapStateDescriptor<String, Long>(
                    "minTimestampTrackerIDDescriptor", // state name
                    BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

            MapStateDescriptor<String, Long> maxTimestampTrackerIDDescriptor = new MapStateDescriptor<String, Long>(
                    "maxTimestampTrackerIDDescriptor", // state name
                    BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

            this.minTimestampTrackerIDMapState = getRuntimeContext().getMapState(minTimestampTrackerIDDescriptor);
            this.maxTimestampTrackerIDMapState = getRuntimeContext().getMapState(maxTimestampTrackerIDDescriptor);
        }

        @Override
        // Outputs a tuple containing cellID, number of objects in the cell and its requested aggregate
        //public Tuple3<String, Integer, HashMap<String, Long>> map(Point p) throws Exception {
        public Tuple4<String, Integer, HashMap<String, Long>, Long> map(Point p) throws Exception {

            // HashMap<TrackerID, timestamp>
            //HashMap<String, Long> minTimestampTrackerID = new HashMap<String, Long>();
            //HashMap<String, Long> maxTimestampTrackerID = new HashMap<String, Long>();
            // HashMap <TrackerID, TrajLength>
            HashMap<String, Long> trackerIDTrajLength = new HashMap<String, Long>();


            // Restoring states from MapStates (if exist)
            /*
            for (Map.Entry<String, Long> entry : minTimestampTrackerIDMapState.entries()) {

                String objID = entry.getKey();
                Long minTimestamp = entry.getValue();
                Long maxTimestamp = maxTimestampTrackerIDMapState.get(objID);

                minTimestampTrackerID.put(objID, minTimestamp);
                maxTimestampTrackerID.put(objID, maxTimestamp);
            }
             */

            // Updating maps using new data/point
            //Long currMinTimestamp_ = minTimestampTrackerID.get(p.objID);
            //Long currMaxTimestamp_ = maxTimestampTrackerID.get(p.objID);
            Long currMinTimestamp_ = minTimestampTrackerIDMapState.get(p.objID);
            Long currMaxTimestamp_ = maxTimestampTrackerIDMapState.get(p.objID);


            /*
            if (currMinTimestamp_ != null) { // If exists update else insert
                if (p.timeStampMillisec < currMinTimestamp_) {
                    minTimestampTrackerID.replace(p.objID, p.timeStampMillisec);
                }

                if (p.timeStampMillisec > currMaxTimestamp_) {
                    maxTimestampTrackerID.replace(p.objID, p.timeStampMillisec);
                }

            } else {
                minTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                maxTimestampTrackerID.put(p.objID, p.timeStampMillisec);
            }*/

            if (currMinTimestamp_ != null) { // If exists update else insert
                if (p.timeStampMillisec < currMinTimestamp_) {
                    minTimestampTrackerIDMapState.put(p.objID, p.timeStampMillisec);
                }

                if (p.timeStampMillisec > currMaxTimestamp_) {
                    maxTimestampTrackerIDMapState.put(p.objID, p.timeStampMillisec);
                }

            } else {  // If does not exist
                minTimestampTrackerIDMapState.put(p.objID, p.timeStampMillisec);
                maxTimestampTrackerIDMapState.put(p.objID, p.timeStampMillisec);
            }

            Date date = new Date();
            Long latency =  date.getTime() - p.ingestionTime;
            //System.out.println(latency);

            // Updating the state variables
            //minTimestampTrackerIDMapState.clear();
            //maxTimestampTrackerIDMapState.clear();
            //minTimestampTrackerIDMapState.putAll(minTimestampTrackerID);
            //maxTimestampTrackerIDMapState.putAll(maxTimestampTrackerID);

            // Generating Output based on aggregateFunction variable
            if(this.aggregateFunction.equalsIgnoreCase("ALL")){

                trackerIDTrajLength.clear();
                for (Map.Entry<String, Long> entry : minTimestampTrackerIDMapState.entries()) {
                    String objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerIDMapState.get(objID);
                    // Deleting halted trajectories
                    if(!deleteHaltedTrajectories(currMaxTimestamp, inactiveTrajDeletionThreshold, objID)) {
                        //Populating results
                        trackerIDTrajLength.put(objID, (currMaxTimestamp - currMinTimestamp));
                    }
                }
                //return Tuple3.of(p.gridID, trackerIDTrajLength.size(), trackerIDTrajLength);
                return Tuple4.of(p.gridID, trackerIDTrajLength.size(), trackerIDTrajLength, latency);

            }
            else if(this.aggregateFunction.equalsIgnoreCase("SUM") || this.aggregateFunction.equalsIgnoreCase("AVG")){

                trackerIDTrajLength.clear();
                long sumTrajLength = 0L;
                int counter = 0;
                for (Map.Entry<String, Long> entry : minTimestampTrackerIDMapState.entries()) {
                    String objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerIDMapState.get(objID);
                    // Deleting halted trajectories
                    if(!deleteHaltedTrajectories(currMaxTimestamp, inactiveTrajDeletionThreshold, objID)) {
                        counter++;
                        sumTrajLength += (currMaxTimestamp - currMinTimestamp);
                    }
                }

                if(this.aggregateFunction.equalsIgnoreCase("SUM"))
                {
                    trackerIDTrajLength.put("", sumTrajLength);
                    //return Tuple3.of(p.gridID, counter, trackerIDTrajLength);
                    return Tuple4.of(p.gridID, counter, trackerIDTrajLength, latency);
                }
                else // AVG
                {
                    Long avgTrajLength = (Long)Math.round((sumTrajLength * 1.0)/(counter * 1.0));
                    trackerIDTrajLength.put("", avgTrajLength);
                    //return Tuple3.of(p.gridID, counter, trackerIDTrajLength);
                    return Tuple4.of(p.gridID, counter, trackerIDTrajLength, latency);
                }
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MIN")){

                long minTrajLength = Long.MAX_VALUE;
                String minTrajLengthObjID = "";
                trackerIDTrajLength.clear();
                int counter = 0;

                for (Map.Entry<String, Long> entry : minTimestampTrackerIDMapState.entries()) {
                    String objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerIDMapState.get(objID);
                    // Deleting halted trajectories
                    if(!deleteHaltedTrajectories(currMaxTimestamp, inactiveTrajDeletionThreshold, objID)) {
                        counter++;
                        long trajLength = currMaxTimestamp - currMinTimestamp;

                        if (trajLength < minTrajLength) {
                            minTrajLength = trajLength;
                            minTrajLengthObjID = objID;
                        }
                    }
                }

                trackerIDTrajLength.put(minTrajLengthObjID, minTrajLength);
                //return Tuple3.of(p.gridID, counter, trackerIDTrajLength);
                return Tuple4.of(p.gridID, counter, trackerIDTrajLength, latency);
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MAX")){

                long maxTrajLength = Long.MIN_VALUE;
                String maxTrajLengthObjID = "";
                trackerIDTrajLength.clear();
                int counter = 0;

                for (Map.Entry<String, Long> entry : minTimestampTrackerIDMapState.entries()) {
                    String objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerIDMapState.get(objID);
                    // Deleting halted trajectories
                    if(!deleteHaltedTrajectories(currMaxTimestamp, inactiveTrajDeletionThreshold, objID)) {
                        counter++;
                        long trajLength = currMaxTimestamp - currMinTimestamp;

                        if (trajLength > maxTrajLength) {
                            maxTrajLength = trajLength;
                            maxTrajLengthObjID = objID;
                        }
                    }
                }

                trackerIDTrajLength.put(maxTrajLengthObjID, maxTrajLength);
                //return Tuple3.of(p.gridID, counter, trackerIDTrajLength);
                return Tuple4.of(p.gridID, counter, trackerIDTrajLength, latency);
            }
            else{

                trackerIDTrajLength.clear();
                for (Map.Entry<String, Long> entry : minTimestampTrackerIDMapState.entries()) {
                    String objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerIDMapState.get(objID);
                    // Deleting halted trajectories
                    if(!deleteHaltedTrajectories(currMaxTimestamp, inactiveTrajDeletionThreshold, objID)) {
                        //Populating results
                        trackerIDTrajLength.put(objID, (currMaxTimestamp - currMinTimestamp));
                    }
                }
                //return Tuple3.of(p.gridID, trackerIDTrajLength.size(), trackerIDTrajLength);
                return Tuple4.of(p.gridID, trackerIDTrajLength.size(), trackerIDTrajLength, latency);
            }

            /*
            if(this.aggregateFunction.equalsIgnoreCase("ALL")){

                trackerIDTrajLength.clear();
                for (Map.Entry<String, Long> entry : minTimestampTrackerID.entrySet()) {
                    String objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerID.get(objID);
                    //Populating results
                    trackerIDTrajLength.put(objID, (currMaxTimestamp-currMinTimestamp));
                }
                return Tuple2.of(minTimestampTrackerID.size(), trackerIDTrajLength));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("SUM") || this.aggregateFunction.equalsIgnoreCase("AVG")){

                trackerIDTrajLength.clear();
                Long sumTrajLength = 0L;
                for (Map.Entry<String, Long> entry : minTimestampTrackerID.entrySet()) {
                    String objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerID.get(objID);

                    sumTrajLength += (currMaxTimestamp-currMinTimestamp);
                }

                if(this.aggregateFunction.equalsIgnoreCase("SUM"))
                {
                    trackerIDTrajLength.put("", sumTrajLength);
                    return Tuple2.of(minTimestampTrackerID.size(), trackerIDTrajLength));
                }
                else // AVG
                {
                    Long avgTrajLength = (Long)Math.round((sumTrajLength * 1.0)/(minTimestampTrackerID.size() * 1.0));
                    trackerIDTrajLength.put("", avgTrajLength);
                    return Tuple2.of(minTimestampTrackerID.size(), trackerIDTrajLength));
                }
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MIN")){

                Long minTrajLength = Long.MAX_VALUE;
                String minTrajLengthObjID = "";
                trackerIDTrajLength.clear();

                for (Map.Entry<String, Long> entry : minTimestampTrackerID.entrySet()) {
                    String objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerID.get(objID);

                    Long trajLength = currMaxTimestamp-currMinTimestamp;

                    if(trajLength < minTrajLength){
                        minTrajLength = trajLength;
                        minTrajLengthObjID = objID;
                    }
                }

                trackerIDTrajLength.put(minTrajLengthObjID, minTrajLength);
                return Tuple2.of(minTimestampTrackerID.size(), trackerIDTrajLength));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MAX")){

                Long maxTrajLength = Long.MIN_VALUE;
                String maxTrajLengthObjID = "";
                trackerIDTrajLength.clear();

                for (Map.Entry<String, Long> entry : minTimestampTrackerID.entrySet()) {
                    String objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerID.get(objID);

                    Long trajLength = currMaxTimestamp-currMinTimestamp;

                    if(trajLength > maxTrajLength){
                        maxTrajLength = trajLength;
                        maxTrajLengthObjID = objID;
                    }
                }

                trackerIDTrajLength.put(maxTrajLengthObjID, maxTrajLength);
                return Tuple2.of(minTimestampTrackerID.size(), trackerIDTrajLength));
            }
            else{
                trackerIDTrajLength.clear();
                for (Map.Entry<String, Long> entry : minTimestampTrackerID.entrySet()) {
                    String objID = entry.getKey();
                    Long currMinTimestamp = entry.getValue();
                    Long currMaxTimestamp = maxTimestampTrackerID.get(objID);

                    trackerIDTrajLength.put(objID, (currMaxTimestamp-currMinTimestamp));
                }
                return Tuple2.of(minTimestampTrackerID.size(), trackerIDTrajLength));
            }
            */
        }

        boolean deleteHaltedTrajectories(Long maxTimestamp, Long maxAllowedLateness, String objID) throws Exception {
            Date date = new Date();

            if (date.getTime() - maxTimestamp > maxAllowedLateness){
                minTimestampTrackerIDMapState.remove(objID);
                maxTimestampTrackerIDMapState.remove(objID);
                return true;
            }
            return false;
        }
    }

    // Count Window Process Function
    //ProcessWindowFunction<IN, OUT, KEY, W extends Window>
    protected class CountWindowProcessFunction extends ProcessWindowFunction<Point, Tuple5<String, Integer, Long, Long, HashMap<String, Long>>, String, GlobalWindow> {

        // HashMap<ObjectID, timestamp>
        HashMap<String, Long> minTimestampTrackerID = new HashMap<String, Long>();
        HashMap<String, Long> maxTimestampTrackerID = new HashMap<String, Long>();
        // HashMap <ObjectID, TrajLength>
        HashMap<String, Long> trackerIDTrajLength = new HashMap<String, Long>();
        HashMap<String, Long> trackerIDTrajLengthOutput = new HashMap<String, Long>();

        String aggregateFunction;
        public CountWindowProcessFunction(String aggregateFunction){
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        // KEY key, Context context, Iterable<IN> elements, Collector<OUT> out
        public void process(String key, Context context, Iterable<Point> input, Collector<Tuple5<String, Integer, Long, Long, HashMap<String, Long>>> output) throws Exception {

            minTimestampTrackerID.clear();
            maxTimestampTrackerID.clear();
            trackerIDTrajLength.clear();
            trackerIDTrajLengthOutput.clear();
            long minTrajLength = Long.MAX_VALUE;
            String minTrajLengthObjID = "";
            long maxTrajLength = Long.MIN_VALUE;
            String maxTrajLengthObjID = "";
            long sumTrajLength = 0L; // Maintains sum of all trajectories of a cell

            for (Point p : input) {
                Long currMinTimestamp = minTimestampTrackerID.get(p.objID);
                Long currMaxTimestamp = maxTimestampTrackerID.get(p.objID);
                Long minTimestamp = currMinTimestamp;
                Long maxTimestamp = currMaxTimestamp;

                if (currMinTimestamp != null) { // If exists replace else insert
                    if (p.timeStampMillisec < currMinTimestamp) {
                        minTimestampTrackerID.replace(p.objID, p.timeStampMillisec);
                        minTimestamp = p.timeStampMillisec;
                    }

                    if (p.timeStampMillisec > currMaxTimestamp) {
                        maxTimestampTrackerID.replace(p.objID, p.timeStampMillisec);
                        maxTimestamp = p.timeStampMillisec;
                    }

                    // Compute the trajectory length and update the map if needed
                    Long currTrajLength = trackerIDTrajLength.get(p.objID);
                    Long trajLength = maxTimestamp - minTimestamp;

                    if (!currTrajLength.equals(trajLength)) { // If exists, replace
                        trackerIDTrajLength.replace(p.objID, trajLength);
                        sumTrajLength -= currTrajLength;
                        sumTrajLength += trajLength;
                    }

                    // Computing MAX Trajectory Length
                    if(trajLength > maxTrajLength){
                        maxTrajLength = trajLength;
                        maxTrajLengthObjID = p.objID;
                    }

                    // Computing MIN Trajectory Length
                    if(trajLength < minTrajLength){
                        minTrajLength = trajLength;
                        minTrajLengthObjID = p.objID;
                    }

                } else { // else insert
                    minTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                    maxTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                    trackerIDTrajLength.put(p.objID, 0L);
                }
            }

            // Tuple5<Key/CellID, #ObjectsInCell, windowStartTime, windowEndTime, Map<TrajId, TrajLength>>
            if(this.aggregateFunction.equalsIgnoreCase("ALL")){
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<String, Long>>(key,
                        trackerIDTrajLength.size(), context.window().maxTimestamp(), context.window().maxTimestamp(), trackerIDTrajLength));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("SUM")){
                if(sumTrajLength > 0) {
                    trackerIDTrajLengthOutput.put("", sumTrajLength);
                    output.collect(new Tuple5<String, Integer, Long, Long, HashMap<String, Long>>(key,
                            trackerIDTrajLength.size(), context.window().maxTimestamp(), context.window().maxTimestamp(), trackerIDTrajLengthOutput));
                }
            }
            else if(this.aggregateFunction.equalsIgnoreCase("AVG")){
                if(sumTrajLength > 0) {
                    Long avgTrajLength = (Long) Math.round((sumTrajLength * 1.0) / (trackerIDTrajLength.size() * 1.0));
                    trackerIDTrajLengthOutput.put("", avgTrajLength);
                    output.collect(new Tuple5<String, Integer, Long, Long, HashMap<String, Long>>(key,
                            trackerIDTrajLength.size(), context.window().maxTimestamp(), context.window().maxTimestamp(), trackerIDTrajLengthOutput));
                }
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MIN")){
                if(minTrajLength != Long.MAX_VALUE) {
                    trackerIDTrajLengthOutput.put(minTrajLengthObjID, minTrajLength);
                    output.collect(new Tuple5<String, Integer, Long, Long, HashMap<String, Long>>(key,
                            trackerIDTrajLength.size(), context.window().maxTimestamp(), context.window().maxTimestamp(), trackerIDTrajLengthOutput));
                }
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MAX")){
                if(maxTrajLength != Long.MIN_VALUE) {
                    trackerIDTrajLengthOutput.put(maxTrajLengthObjID, maxTrajLength);
                    output.collect(new Tuple5<String, Integer, Long, Long, HashMap<String, Long>>(key,
                            trackerIDTrajLength.size(), context.window().maxTimestamp(), context.window().maxTimestamp(), trackerIDTrajLengthOutput));
                }
            }
            else{
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<String, Long>>(key,
                        trackerIDTrajLength.size(), context.window().maxTimestamp(), context.window().maxTimestamp(), trackerIDTrajLength));
            }
        }
    }

    //Time Window Process Function
    //ProcessWindowFunction<IN, OUT, KEY, W extends Window>
    protected class TimeWindowProcessFunction extends ProcessWindowFunction<Point, Tuple5<String, Integer, Long, Long, HashMap<String, Long>>, String, TimeWindow> {

        // HashMap<TrackerID, timestamp>
        HashMap<String, Long> minTimestampTrackerID = new HashMap<String, Long>();
        HashMap<String, Long> maxTimestampTrackerID = new HashMap<String, Long>();
        // HashMap <TrackerID, TrajLength>
        HashMap<String, Long> trackerIDTrajLength = new HashMap<String, Long>();
        HashMap<String, Long> trackerIDTrajLengthOutput = new HashMap<String, Long>();

        String aggregateFunction;
        public TimeWindowProcessFunction(String aggregateFunction){
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        // KEY key, Context context, Iterable<IN> elements, Collector<OUT> out
        public void process(String key, Context context, Iterable<Point> input, Collector<Tuple5<String, Integer, Long, Long, HashMap<String, Long>>> output) throws Exception {

            minTimestampTrackerID.clear();
            maxTimestampTrackerID.clear();
            trackerIDTrajLength.clear();
            trackerIDTrajLengthOutput.clear();
            long minTrajLength = Long.MAX_VALUE;
            String minTrajLengthObjID = "";
            long maxTrajLength = Long.MIN_VALUE;
            String maxTrajLengthObjID = "";
            long sumTrajLength = 0L;

            // Iterate through all the points corresponding to a single grid-cell within the scope of the window
            for (Point p : input) {
                Long currMinTimestamp = minTimestampTrackerID.get(p.objID);
                Long currMaxTimestamp = maxTimestampTrackerID.get(p.objID);
                Long minTimestamp = currMinTimestamp;
                Long maxTimestamp = currMaxTimestamp;

                //System.out.println("point timestamp " + p.timeStampMillisec + " Window bounds: " + context.window().getStart() + ", " + context.window().getEnd());

                if (currMinTimestamp != null) { // If exists replace else insert
                    if (p.timeStampMillisec < currMinTimestamp) {
                        minTimestampTrackerID.replace(p.objID, p.timeStampMillisec);
                        minTimestamp = p.timeStampMillisec;
                    }

                    if (p.timeStampMillisec > currMaxTimestamp) {
                        maxTimestampTrackerID.replace(p.objID, p.timeStampMillisec);
                        maxTimestamp = p.timeStampMillisec;
                    }

                    // Compute the trajectory length and update the map if needed
                    Long currTrajLength = trackerIDTrajLength.get(p.objID);
                    Long trajLength = maxTimestamp - minTimestamp;

                    if (!currTrajLength.equals(trajLength)) {
                        trackerIDTrajLength.replace(p.objID, trajLength);
                        sumTrajLength -= currTrajLength;
                        sumTrajLength += trajLength;
                    }

                    // Computing MAX Trajectory Length
                    if(trajLength > maxTrajLength){
                        maxTrajLength = trajLength;
                        maxTrajLengthObjID = p.objID;
                    }

                    // Computing MIN Trajectory Length
                    if(trajLength < minTrajLength){
                        minTrajLength = trajLength;
                        minTrajLengthObjID = p.objID;
                    }

                } else {
                    minTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                    maxTimestampTrackerID.put(p.objID, p.timeStampMillisec);
                    trackerIDTrajLength.put(p.objID, 0L);
                }
            }

            // Tuple5<Key/CellID, #ObjectsInCell, windowStartTime, windowEndTime, Map<TrajId, TrajLength>>
            if(this.aggregateFunction.equalsIgnoreCase("ALL")){
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<String, Long>>(key,
                        trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLength));
            }
            else if(this.aggregateFunction.equalsIgnoreCase("SUM")){
                if(sumTrajLength > 0) {
                    trackerIDTrajLengthOutput.put("", sumTrajLength);
                    output.collect(new Tuple5<String, Integer, Long, Long, HashMap<String, Long>>(key,
                            trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLengthOutput));
                }
            }
            else if(this.aggregateFunction.equalsIgnoreCase("AVG")){
                if(sumTrajLength > 0) {
                    Long avgTrajLength = (Long) Math.round((sumTrajLength * 1.0) / (trackerIDTrajLength.size() * 1.0));
                    trackerIDTrajLengthOutput.put("", avgTrajLength);
                    output.collect(new Tuple5<String, Integer, Long, Long, HashMap<String, Long>>(key,
                            trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLengthOutput));
                }
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MIN")){
                if(minTrajLength != Long.MAX_VALUE) {
                    trackerIDTrajLengthOutput.put(minTrajLengthObjID, minTrajLength);
                    output.collect(new Tuple5<String, Integer, Long, Long, HashMap<String, Long>>(key,
                            trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLengthOutput));
                }
            }
            else if(this.aggregateFunction.equalsIgnoreCase("MAX")){
                if(maxTrajLength != Long.MIN_VALUE) {
                    trackerIDTrajLengthOutput.put(maxTrajLengthObjID, maxTrajLength);
                    output.collect(new Tuple5<String, Integer, Long, Long, HashMap<String, Long>>(key,
                            trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLengthOutput));
                }
            }
            else{
                output.collect(new Tuple5<String, Integer, Long, Long, HashMap<String, Long>>(key,
                        trackerIDTrajLength.size(), context.window().getStart(), context.window().getEnd(), trackerIDTrajLength));
            }
        }
    }
}
