package GeoFlink.apps;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.shaded.guava18.com.google.common.collect.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.locationtech.jts.geom.*;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class StayTime implements Serializable {

    //--------------- CellStayTime - Point Stream - Window-based -----------------//
    public static DataStream<Tuple2<String, Double>> CellStayTime(DataStream<Point> pointStream, Set<String> trajIDSet, int allowedLateness, int windowSize, int windowSlideStep, UniformGrid uGrid){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
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

        //filteredStream.print();

        return filteredStream.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
        }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new CellStayTimeWinFunction(uGrid))
                .keyBy(new KeySelector<Tuple5<String, Long, Long, String, Double>, String>() {
                    @Override
                    public String getKey(Tuple5<String, Long, Long, String, Double> tuple5) throws Exception {
                        return tuple5.f3;
                    }
                }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new CellStayTimeAggregateWinFunction());
    }



    //--------------- Cell SensorRange Intersection- Window-based -----------------//
    public static DataStream<Tuple2<String, Integer>> CellSensorRangeIntersection(DataStream<Polygon> inputStream, Set<String> trajIDSet, int allowedLateness, int windowSize, int windowSlideStep, UniformGrid uGrid){

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Polygon> streamWithTsAndWm =
                inputStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Polygon p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Polygon> filteredStream = streamWithTsAndWm.filter(new FilterFunction<Polygon>() {
            @Override
            public boolean filter(Polygon p) throws Exception {
                if (trajIDSet.size() > 0)
                    return ((trajIDSet.contains(p.objID)));
                else
                    return true;
            }
        });

        // Generating a replicated polygon stream with respect to Grid ID Set
        DataStream<Polygon> replicatedStream = filteredStream.flatMap(new HelperClass.ReplicatePolygonStreamUsingObjID());

        return replicatedStream.keyBy(new KeySelector<Polygon, String>() {
            @Override
            public String getKey(Polygon p) throws Exception {
                return p.gridID;
            }
        }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new CellSensorIntersectionWinFunction(uGrid));
    }

    //--------------- Cell SensorRange Intersection- Window-based -----------------//
    public static DataStream<Tuple4<String, Long, Long, Double>> normalizedCellStayTime(DataStream<Point> movingPointStream, Set<String> trajIDSetPoint, DataStream<Polygon> sensorRangeStream, Set<String> trajIDSetSensorRange,  int allowedLateness, int windowSize, int windowSlideStep, UniformGrid uGrid){

        DataStream<Tuple2<String, Double>> cellStayTime = CellStayTime(movingPointStream, trajIDSetPoint, allowedLateness, windowSize, windowSlideStep, uGrid);
        DataStream<Tuple2<String, Integer>> cellSensorRangeIntersection = CellSensorRangeIntersection(sensorRangeStream, trajIDSetSensorRange, allowedLateness, windowSize, windowSlideStep, uGrid);

        //cellStayTime.print();
        //cellSensorRangeIntersection.print();

        return cellStayTime.join(cellSensorRangeIntersection)
        .where(new KeySelector<Tuple2<String, Double>, String>() {
            @Override
            public String getKey(Tuple2<String, Double> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).equalTo(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
        .apply(new normalizedCellStayTimeWinFunction(windowSize))
        .keyBy(
                new KeySelector<Tuple2<String, Double>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Double> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
        // Second window is used to get the window timestamp boundaries
        .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
        .apply(new RichWindowFunction<Tuple2<String, Double>, Tuple4<String, Long, Long, Double>, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow timeWindow, Iterable<Tuple2<String, Double>> iterable, Collector<Tuple4<String, Long, Long, Double>> collector) throws Exception {
                for(Tuple2<String, Double> element : iterable)
                {
                    collector.collect(Tuple4.of(element.f0, timeWindow.getStart(), timeWindow.getEnd(), element.f1));
                }
            }
        });

                /*
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep))).apply(new AllWindowFunction<Tuple4<String, Long, Long, Double>, Tuple4<String, Long, Long, Double>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple4<String, Long, Long, Double>> iterable, Collector<Tuple4<String, Long, Long, Double>> collector) throws Exception {
                        for(Tuple4<String, Long, Long, Double> element : iterable)
                        {
                            collector.collect(Tuple4.of(element.f0, timeWindow.getStart(), timeWindow.getEnd(), element.f3));
                        }
                    }
                });*/

                /*
        return cellStayTime.keyBy(new KeySelector<Tuple4<String, Long, Long, Double>, String>() {
                    @Override
                    public String getKey(Tuple4<String, Long, Long, Double> tuple4) throws Exception {
                        return tuple4.f0;
                    }
                }).intervalJoin(cellSensorRangeIntersection.keyBy(new KeySelector<Tuple4<String, Long, Long, Integer>, String>() {
                    @Override
                    public String getKey(Tuple4<String, Long, Long, Integer> tuple4) throws Exception {
                        return tuple4.f0;
                    }
                })).between(Time.seconds(-10), Time.seconds(1)).process(
                        new ProcessJoinFunction<Tuple4<String, Long, Long, Double>, Tuple4<String, Long, Long, Integer>, Tuple4<String, Long, Long, Double>>() {
                             @Override
                             public void processElement(Tuple4<String, Long, Long, Double> stringLongLongDoubleTuple4, Tuple4<String, Long, Long, Integer> stringLongLongIntegerTuple4, Context context, Collector<Tuple4<String, Long, Long, Double>> collector) throws Exception {

                             }
                        }
        );

                 */
    }

    //normalizedCellStayTimeWinFunction RichWindowFunction<IN, OUT, KEY, W>
    //public static class normalizedCellStayTimeWinFunction extends RichJoinFunction<Tuple4<String, Long, Long, Double>, Tuple4<String, Long, Long, Integer>, Tuple4<String, Long, Long, Double>> {
        public static class normalizedCellStayTimeWinFunction extends RichJoinFunction<Tuple2<String, Double>, Tuple2<String, Integer>, Tuple2<String, Double>> {

        int windowSize_;
        public  normalizedCellStayTimeWinFunction() {}

        public  normalizedCellStayTimeWinFunction(int windowSize) {
            this.windowSize_ = windowSize;
        }

        @Override
        public Tuple2<String, Double> join(Tuple2<String, Double> stayTime, Tuple2<String, Integer> sensorRangeIntersection) throws Exception {

            double normalizedStayTime = ((stayTime.f1/1000)/sensorRangeIntersection.f1)*this.windowSize_;

            /*
            if ((stayTime.f1 != sensorRangeIntersection.f1) || (stayTime.f2 != sensorRangeIntersection.f2)) {
                System.out.println("traj win, sensor win: " + stayTime.f1 + ", " + sensorRangeIntersection.f1 + ", " + stayTime.f2 + ", " + sensorRangeIntersection.f2);
                //System.out.println("sensor range time window: " + ", " + sensorRangeIntersection.f1 + ", " + sensorRangeIntersection.f2);
                System.out.println("");
            }
             */

            return Tuple2.of(stayTime.f0, normalizedStayTime);
        }
    }

    //CellStayTimeWinFunction RichWindowFunction<IN, OUT, KEY, W>
    public static class CellStayTimeWinFunction extends RichWindowFunction<Point, Tuple5<String, Long, Long, String, Double>, String, TimeWindow> {

        UniformGrid uGrid;
        //ctor
        public  CellStayTimeWinFunction() {};

        public  CellStayTimeWinFunction(UniformGrid uniformGrid) {
            this.uGrid = uniformGrid;
        };

        @Override
        public void apply(String key, TimeWindow window, Iterable<Point> input, Collector<Tuple5<String, Long, Long, String, Double>> output) throws Exception {

            Multimap<Long, Point> timeStampPointMap = TreeMultimap.create(Ordering.natural(), Ordering.arbitrary()); // Ordering is maintained for keys, avoid duplicates in values
            //Multimap<String, Double> cellStayTime = LinkedListMultimap.create(); // Ordering is not maintained

            // Atrributes order: objID, startingTimestamp (lastTimestamp), currentTimestamp, cellID, stayTime
            Multimap<String, Tuple4<Long, Long, String, Double>> cellStayTimeMap = LinkedListMultimap.create(); // Ordering is not maintained

            org.locationtech.jts.geom.Coordinate lastCoordinate = new Coordinate(0,0,0);

            Long lastTimestamp = Long.MIN_VALUE;
            double timeDiff;
            String lastCellIDStr = "";
            Boolean firstPoint = true;

            // Insert all points in the window into timeStampPointMap for sorting with respect to timestamp
            for (Point p : input) {
                timeStampPointMap.put(p.timeStampMillisec, p);
            }

            // Computing staytime of consecutive points with respect to timestamp
            for (Point p : timeStampPointMap.values()){

                if(firstPoint){
                    firstPoint = false;
                    lastTimestamp = p.timeStampMillisec;
                    lastCellIDStr = p.gridID;
                    lastCoordinate.setX(p.point.getX());
                    lastCoordinate.setY(p.point.getY());
                    lastCoordinate.setZ(0);
                    //lastCoordinate = new Coordinate(p.point.getX(), p.point.getY(), 0);
                }
                else{
                    // Time difference between 2 consecutive points to calculate staytime
                    timeDiff = p.timeStampMillisec - lastTimestamp;

                    ArrayList<Integer> lastCellIndices = HelperClass.getIntCellIndices(lastCellIDStr);
                    ArrayList<Integer> currentCellIndices = HelperClass.getIntCellIndices(p.gridID);

                    //Coordinate[] cellBoundary1 = uGrid.getCellMinMaxBoundary(p.gridID);
                    //System.out.println("val " + p.gridID + ", " + cellBoundary1[0].getX() + ", " + cellBoundary1[0].getY() + ", " + cellBoundary1[1].getX() + ", " + cellBoundary1[1].getY());

                    // case 0: if both the indices are same
                    if(lastCellIDStr.equals(p.gridID)){
                        //cellStayTime.put(lastCellIDStr, timeDiff);
                        cellStayTimeMap.put(p.objID, Tuple4.of(lastTimestamp, p.timeStampMillisec, p.gridID, timeDiff));
                        //System.out.println("case0 " + cellStayTime.toString());
                    }
                    // case 1: if one of the indices are same: x-index
                    else if(currentCellIndices.get(0).equals(lastCellIndices.get(0))){
                        Integer cellDiff;
                        if(currentCellIndices.get(1) > lastCellIndices.get(1)){

                            cellDiff = (currentCellIndices.get(1) - lastCellIndices.get(1)) + 1;

                            for (int i = lastCellIndices.get(1); i <= currentCellIndices.get(1); i++) {
                                String gridCellIDStr = HelperClass.generateCellIDStr(currentCellIndices.get(0), i, uGrid);
                                //cellStayTime.put(gridCellIDStr, timeDiff/cellDiff);
                                cellStayTimeMap.put(p.objID, Tuple4.of(lastTimestamp, p.timeStampMillisec, gridCellIDStr, timeDiff/cellDiff));
                            }
                            //System.out.println("case1: cellDiff: " + cellDiff + ", " + cellStayTime.toString());

                        }else{
                            cellDiff = (lastCellIndices.get(1) - currentCellIndices.get(1)) + 1;

                            for (int i = currentCellIndices.get(1); i <= lastCellIndices.get(1); i++) {
                                String gridCellIDStr = HelperClass.generateCellIDStr(currentCellIndices.get(0), i, uGrid);
                                //cellStayTime.put(gridCellIDStr, timeDiff/cellDiff);
                                cellStayTimeMap.put(p.objID, Tuple4.of(lastTimestamp, p.timeStampMillisec, gridCellIDStr, timeDiff/cellDiff));
                            }
                            //System.out.println("case1: cellDiff: " + cellDiff + ", " + cellStayTime.toString());
                        }
                    }
                    // case 1: if one of the indices are same: y-index
                    else if(currentCellIndices.get(1).equals(lastCellIndices.get(1))){
                        Integer cellDiff;
                        if(currentCellIndices.get(0) > lastCellIndices.get(0)){

                            cellDiff = (currentCellIndices.get(0) - lastCellIndices.get(0)) + 1;

                            for (int i = lastCellIndices.get(0); i <= currentCellIndices.get(0); i++) {
                                String gridCellIDStr = HelperClass.generateCellIDStr(i, currentCellIndices.get(1), uGrid);
                                //cellStayTime.put(gridCellIDStr, timeDiff/cellDiff);
                                cellStayTimeMap.put(p.objID, Tuple4.of(lastTimestamp, p.timeStampMillisec, gridCellIDStr, timeDiff/cellDiff));
                            }
                            //System.out.println("case1: cellDiff: " + cellDiff + ", " + cellStayTime.toString());

                        }else{
                            cellDiff = (lastCellIndices.get(0) - currentCellIndices.get(0)) + 1;

                            for (int i = currentCellIndices.get(0); i <= lastCellIndices.get(0); i++) {
                                String gridCellIDStr = HelperClass.generateCellIDStr(i, currentCellIndices.get(1), uGrid);
                                //cellStayTime.put(gridCellIDStr, timeDiff/cellDiff);
                                cellStayTimeMap.put(p.objID, Tuple4.of(lastTimestamp, p.timeStampMillisec, gridCellIDStr, timeDiff/cellDiff));
                            }
                            //System.out.println("case1: cellDiff: " + cellDiff + ", " + cellStayTime.toString());
                        }
                    }
                    // case 2: if both the indices are different
                    else{
                        GeometryFactory geofact = new GeometryFactory();

                        // Create a line segment using last point and current point
                        Coordinate currCoordinate = new Coordinate(p.point.getX(),p.point.getY(),0);
                        Coordinate[] lineStringCoordinates = {lastCoordinate, currCoordinate};
                        LineString jtsLineString = geofact.createLineString(lineStringCoordinates);

                        GeoFlink.spatialObjects.LineString lineString = new GeoFlink.spatialObjects.LineString("0", jtsLineString, uGrid);

                        // gridIDsSet contains all the possible gridIDs the line segment can belong
                        HashSet<String> gridIDsSet = lineString.gridIDsSet;

                        // set to contain intersecting grid cells
                        Set<String> intersectingCells = new HashSet<>();
                        // adding the two gridIDs that contain the two extremes of line segment
                        intersectingCells.add(lastCellIDStr);
                        intersectingCells.add(p.gridID);

                        // Iterate through all the cells/polygons to check if it intersects with the line
                        for(String gridID: gridIDsSet){

                            // ignoring the cellIDs already added
                            if(gridID.equals(lastCellIDStr) || gridID.equals(p.gridID)){
                                continue;
                            }

                            // get the cell min,max-boundary
                            Coordinate[] cellBoundary = uGrid.getCellMinMaxBoundary(gridID);
                            org.locationtech.jts.geom.Polygon cellPoly = HelperClass.generatePolygonUsingBBox(cellBoundary);

                            if(cellPoly != null){
                                if (cellPoly.intersects(jtsLineString)) {
                                    intersectingCells.add(gridID);
                                }
                            }
                        }

                        // Iterating through all the intersecting cells to add their staytime
                        for(String cellIDStr: intersectingCells){
                            //cellStayTime.put(cellIDStr, timeDiff/intersectingCells.size());
                            cellStayTimeMap.put(p.objID, Tuple4.of(lastTimestamp, p.timeStampMillisec, cellIDStr, timeDiff/intersectingCells.size()));
                        }
                        //System.out.println("case2, intersectingCells size: " + intersectingCells.size() + "," + cellStayTime.toString());
                    }

                    // Update variables
                    lastTimestamp = p.timeStampMillisec;
                    lastCellIDStr = p.gridID;
                    lastCoordinate.setX(p.point.getX());
                    lastCoordinate.setY(p.point.getY());
                    lastCoordinate.setZ(0);
                    //lastCoordinate = new Coordinate(p.point.getX(), p.point.getY(), 0);
                }
                //System.out.println(p);
                //System.out.println(p.timeStampMillisec);
            }
            //System.out.println(cellStayTimeMap.toString());
            cellStayTimeMap.asMap().forEach((mapKey, collection) -> {
                //System.out.println("Object ID:" + mapKey);
                // Tuple4<lastTimestamp, currentTimestamp, cellID, stayTime>
                for(Tuple4<Long, Long, String, Double> x:collection){
                    //System.out.println(x);
                    output.collect(Tuple5.of(mapKey, x.f0, x.f1, x.f2, x.f3));
                }
            });
            // cellStayTimeMap: <ObjectID(TrajID), Tuple4<lastTimestamp, currentTimestamp, cellID, stayTime>>
            //output.collect(cellStayTimeMap);
        }
    }

    //CellSensorIntersection RichWindowFunction<IN, OUT, KEY, W>
    public static class CellSensorIntersectionWinFunction extends RichWindowFunction<Polygon, Tuple2<String, Integer>, String, TimeWindow> {

        UniformGrid uGrid;
        //ctor
        public  CellSensorIntersectionWinFunction() {};

        public  CellSensorIntersectionWinFunction(UniformGrid uniformGrid) {
            this.uGrid = uniformGrid;
        };

        @Override
        public void apply(String cellID, TimeWindow window, Iterable<Polygon> input, Collector<Tuple2<String, Integer>> out) throws Exception {

            //Map<Long, Polygon> timestampPolygonMap = new HashMap<>();
            Multimap<Long, Polygon> timestampPolygonMap = TreeMultimap.create(Ordering.natural(), Ordering.arbitrary()); // Ordering is maintained for keys, avoid duplicates in values

            // Generating polygon corresponding to cell boundary
            Coordinate[] cellBoundary = uGrid.getCellMinMaxBoundary(cellID);
            org.locationtech.jts.geom.Polygon cellPoly = HelperClass.generatePolygonUsingBBox(cellBoundary);

            for (Polygon p : input) {
                if(cellPoly != null) {
                    if (cellPoly.intersects(p.polygon)) {
                        timestampPolygonMap.put(p.timeStampMillisec, p);
                    }
                }
            }

            if(timestampPolygonMap.keySet().size() > 0) {
                out.collect(Tuple2.of(cellID, timestampPolygonMap.keySet().size()));
            }
        }
    }

    // class CellStayTimeAggregateWinFunction window function
    public static class CellStayTimeAggregateWinFunction extends RichWindowFunction<Tuple5<String, Long, Long, String, Double>, Tuple2<String, Double>, String, TimeWindow>{

        //ctor
        public CellStayTimeAggregateWinFunction(){}

        @Override
        public void apply(String cellID, TimeWindow timeWindow, Iterable<Tuple5<String, Long, Long, String, Double>> input, Collector<Tuple2<String, Double>> output) throws Exception {

            double sumStayTime = 0;
            for(Tuple5<String, Long, Long, String, Double> tuple5: input){
                sumStayTime += tuple5.f4;
            }
            output.collect(Tuple2.of(cellID, sumStayTime));
        }
    }

    // Kafka Output Schema
    public static class normalizedStayTimeOutputSchema implements Serializable, KafkaSerializationSchema<Tuple4<String, Long, Long, Double>> {

        String outputTopic;
        Integer queryID;

        public normalizedStayTimeOutputSchema(String outputTopicName)
        {
            this.outputTopic = outputTopicName;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Tuple4<String, Long, Long, Double> element, @Nullable Long timestamp) {

            /* required output format
            {
                "cellid": "0000100002",
                "window": [123456,223145],
                "normalize": 0.002
            }
         */
            JSONObject jsonObj = new JSONObject();

            jsonObj.put("cellID", element.f0);
            JSONArray window = new JSONArray();
            window.put(element.f1);
            window.put(element.f2);
            jsonObj.put("window", window);
            jsonObj.put("normalizedStayTime", element.f3);

            //System.out.println(jsonObj);
            return new ProducerRecord<byte[], byte[]>(outputTopic, jsonObj.toString().getBytes(StandardCharsets.UTF_8));
            //return new ProducerRecord<byte[], byte[]>(outputTopic, element.f0.getBytes(StandardCharsets.UTF_8), outputStr.getBytes(StandardCharsets.UTF_8));
        }
    }


}