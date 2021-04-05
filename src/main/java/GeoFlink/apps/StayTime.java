package GeoFlink.apps;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.TStatsQuery;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.guava18.com.google.common.collect.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StayTime implements Serializable {

    //--------------- TStatsQuery QUERY - Window-based -----------------//
    public static DataStream<Multimap<String, Tuple4<Long, Long, String, Double>>> CellStayTime(DataStream<Point> pointStream, Set<String> trajIDSet, int allowedLateness, int windowSize, int windowSlideStep, UniformGrid uGrid){

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
                .apply(new CellStayTimeWinFunction(uGrid));
    }

    //RichWindowFunction<IN, OUT, KEY, W>
    public static class CellStayTimeWinFunction extends RichWindowFunction<Point, Multimap<String, Tuple4<Long, Long, String, Double>>, String, TimeWindow> {

        UniformGrid uGrid;
        //ctor
        public  CellStayTimeWinFunction() {};

        public  CellStayTimeWinFunction(UniformGrid uniformGrid) {
            this.uGrid = uniformGrid;
        };

        @Override
        public void apply(String key, TimeWindow window, Iterable<Point> input, Collector<Multimap<String, Tuple4<Long, Long, String, Double>>> output) throws Exception {

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

                    //Coordinate[] cellBoundary1 = uGrid.getCellBoundary(p.gridID);
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
                            Coordinate[] cellBoundary = uGrid.getCellBoundary(gridID);

                            // creating a cell polygon for the sake of computing the intersection between polygon and line segment
                            if(cellBoundary.length > 0){

                                Coordinate c1 = new Coordinate(cellBoundary[0].getX(), cellBoundary[0].getY(), 0);
                                Coordinate c2 = new Coordinate(cellBoundary[1].getX(), cellBoundary[0].getY(), 0);
                                Coordinate c3 = new Coordinate(cellBoundary[1].getX(), cellBoundary[1].getY(), 0);
                                Coordinate c4 = new Coordinate(cellBoundary[0].getX(), cellBoundary[1].getY(), 0);

                                Coordinate[] polygonCoordinates;
                                polygonCoordinates = new Coordinate[]{c1, c2, c3, c4, c1};
                                Polygon cellPoly = geofact.createPolygon(polygonCoordinates);

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
                System.out.println("Object ID:" + mapKey);
                for(Tuple4<Long, Long, String, Double> x:collection){
                    System.out.println(x);
                }
            });
            output.collect(cellStayTimeMap);
        }
    }

}
