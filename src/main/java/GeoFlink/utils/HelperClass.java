/*
Copyright 2020 Data Platform Research Team, AIRC, AIST, Japan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package GeoFlink.utils;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HelperClass {

    private static final double mEarthRadius = 6371008.7714;


    // return a string padded with zeroes to make the string equivalent to desiredStringLength
    public static String padLeadingZeroesToInt(int cellIndex, int desiredStringLength)
    {
        return String.format("%0"+ Integer.toString(desiredStringLength) +"d", cellIndex);
    }

    // return an integer by removing the leading zeroes from the string
    public static int removeLeadingZeroesFromString(String str)
    {
        return Integer.parseInt(str.replaceFirst("^0+(?!$)", ""));
    }

    public static boolean pointWithinQueryRange(ArrayList<Integer> pointCellIndices, ArrayList<Integer> queryCellIndices, int neighboringLayers){

        if((pointCellIndices.get(0) >= queryCellIndices.get(0) - neighboringLayers) && (pointCellIndices.get(0) <= queryCellIndices.get(0) + neighboringLayers) && (pointCellIndices.get(1) >= queryCellIndices.get(1) - neighboringLayers) && (pointCellIndices.get(1) <= queryCellIndices.get(1) + neighboringLayers)){
            return true;
        }
        else{
            return false;
        }
    }

    // Compute the Bounding Box of a polygon
    public static Tuple2<Coordinate, Coordinate> getBoundingBox(org.locationtech.jts.geom.Polygon poly)
    {
        // return 2 coordinates, smaller first and larger second
        return Tuple2.of(new Coordinate(poly.getEnvelopeInternal().getMinX(), poly.getEnvelopeInternal().getMinY(), 0), new Coordinate(poly.getEnvelopeInternal().getMaxX(), poly.getEnvelopeInternal().getMaxY(), 0));
    }

    // Compute the Bounding Box of a lineString
    public static Tuple2<Coordinate, Coordinate> getBoundingBox(org.locationtech.jts.geom.LineString lineString)
    {
        // return 2 coordinates, smaller first and larger second
        return Tuple2.of(new Coordinate(lineString.getEnvelopeInternal().getMinX(), lineString.getEnvelopeInternal().getMinY(), 0), new Coordinate(lineString.getEnvelopeInternal().getMaxX(), lineString.getEnvelopeInternal().getMaxY(), 0));
    }

    // Compute the Bounding Box of a MultiPoint
    public static Tuple2<Coordinate, Coordinate> getBoundingBox(org.locationtech.jts.geom.MultiPoint multiPoint)
    {
        // return 2 coordinates, smaller first and larger second
        return Tuple2.of(new Coordinate(multiPoint.getEnvelopeInternal().getMinX(), multiPoint.getEnvelopeInternal().getMinY(), 0), new Coordinate(multiPoint.getEnvelopeInternal().getMaxX(), multiPoint.getEnvelopeInternal().getMaxY(), 0));
    }

    // Compute the Bounding Box of a geometryCollection
    public static Tuple2<Coordinate, Coordinate> getBoundingBox(org.locationtech.jts.geom.GeometryCollection geometryCollection)
    {
        // return 2 coordinates, smaller first and larger second
        return Tuple2.of(new Coordinate(geometryCollection.getEnvelopeInternal().getMinX(), geometryCollection.getEnvelopeInternal().getMinY(), 0), new Coordinate(geometryCollection.getEnvelopeInternal().getMaxX(), geometryCollection.getEnvelopeInternal().getMaxY(), 0));
    }

    // assigning grid cell ID
    public static String assignGridCellID(Coordinate coordinate, UniformGrid uGrid) {

        // Direct approach to compute the cellIDs (Key)
        // int xCellIndex = (int)(Math.floor((point.getX() - uGrid.getMinX())/uGrid.getCellLength()));
        // int yCellIndex = (int)(Math.floor((point.getY() - uGrid.getMinY())/uGrid.getCellLength()));
        int xCellIndex = (int)(Math.floor((coordinate.getX() - uGrid.getMinX())/uGrid.getCellLength()));
        int yCellIndex = (int)(Math.floor((coordinate.getY() - uGrid.getMinY())/uGrid.getCellLength()));

        //String gridIDStr = HelperClass.padLeadingZeroesToInt(xCellIndex, uGrid.getCellIndexStrLength()) + HelperClass.padLeadingZeroesToInt(yCellIndex, uGrid.getCellIndexStrLength());
        String gridIDStr = HelperClass.generateCellIDStr(xCellIndex, yCellIndex, uGrid);

        return gridIDStr;
    }

    public static String generateCellIDStr(int xCellIndex, int yCellIndex, UniformGrid uGrid){
        return HelperClass.padLeadingZeroesToInt(xCellIndex, uGrid.getCellIndexStrLength()) + HelperClass.padLeadingZeroesToInt(yCellIndex, uGrid.getCellIndexStrLength());
    }

    // assigning grid cell ID - BoundingBox
    public static HashSet<String> assignGridCellID(Tuple2<Coordinate, Coordinate> bBox, UniformGrid uGrid) {

        HashSet<String> gridCellIDs = new HashSet<String>();

        // bottom-left coordinate (min values)
        int xCellIndex1 = (int) (Math.floor((bBox.f0.getX() - uGrid.getMinX()) / uGrid.getCellLength()));
        int yCellIndex1 = (int) (Math.floor((bBox.f0.getY() - uGrid.getMinY()) / uGrid.getCellLength()));

        // top-right coordinate (max values)
        int xCellIndex2 = (int) (Math.floor((bBox.f1.getX() - uGrid.getMinX()) / uGrid.getCellLength()));
        int yCellIndex2 = (int) (Math.floor((bBox.f1.getY() - uGrid.getMinY()) / uGrid.getCellLength()));

        for(int x = xCellIndex1; x <= xCellIndex2; x++)
            for(int y = yCellIndex1; y <= yCellIndex2; y++)
            {
                String gridIDStr = HelperClass.padLeadingZeroesToInt(x, uGrid.getCellIndexStrLength()) + HelperClass.padLeadingZeroesToInt(y, uGrid.getCellIndexStrLength());
                gridCellIDs.add(gridIDStr);
            }

        return gridCellIDs;
    }

    public static List<Coordinate> getCoordinates(String inputValue) {
        //"[100.0, 0.0], [103.0, 0.0], [103.0, 1.0], [102.0, 1.0], [100.0, 0.0]"
        List<Coordinate> list = new ArrayList<Coordinate>();
        if (inputValue == null) {
            return list;
        }
        Pattern pattern = Pattern.compile("\\[(.+?)\\]");
        Matcher matcher = pattern.matcher(inputValue);
        while (matcher.find()) {
            try {
                String[] arr = matcher.group(1).trim().split("\\s*,\\s*");
                list.add(new Coordinate(Double.parseDouble(arr[0]), Double.parseDouble(arr[1])));
            }
            catch (Exception e) {}
        }
        return list;
    }

    public static List<List<Coordinate>> getListCoordinates(String inputValue) {
        //"[[116.5, 40.5], [117.6, 40.5], [117.6, 41.4]], [[117.5, 40.5], [118.6, 40.5], [118.6, 41.4]]"
        List<List<Coordinate>> list = new ArrayList<>();
        if (inputValue == null) {
            return list;
        }
        Pattern pattern = Pattern.compile("\\[\\[(.+?)\\]\\]");
        Matcher matcher = pattern.matcher(inputValue);
        while (matcher.find()) {
            try {
                String str = "[" + matcher.group(1).trim() + "]";
                list.add(getCoordinates(str));
            }
            catch (Exception e) {}
        }
        return list;
    }

    public static List<List<List<Coordinate>>> getListListCoordinates(String inputValue) {
        //"[[[116.5, 40.5], [117.6, 40.5], [117.6, 41.4], [116.5, 41.4], [116.5, 40.5]]] , [[[117.5, 40.5], [118.6, 40.5], [118.6, 41.4], [117.5, 41.4], [117.5, 40.5]]] , [[[116.5, 40.5], [117.6, 40.5], [117.6, 41.4], [116.5, 41.4], [116.5, 40.5]]]"
        List<List<List<Coordinate>>> retList = new ArrayList<>();
        if (inputValue == null) {
            return retList;
        }
        Pattern pattern = Pattern.compile("\\[\\[\\[(.+?)\\]\\]\\]");
        Matcher matcher = pattern.matcher(inputValue);
        while (matcher.find()) {
            try {
                String str = "[" + matcher.group(1).trim() + "]";
                List<Coordinate> list = getCoordinates(str);
                List<List<Coordinate>> listList = new ArrayList<>();
                listList.add(list);
                retList.add(listList);
            }
            catch (Exception e) {}
        }
        return retList;
    }

    public static List<String> getParametersArray(String inputValue) {
        //"[timestamp, oID]"
        List<String> list = new ArrayList<String>();
        if (inputValue == null) {
            return list;
        }

        Pattern pattern = Pattern.compile("\\[(.+?)\\]");
        Matcher matcher = pattern.matcher(inputValue);

        while (matcher.find()) {
        try {
            String[] arr = matcher.group(1).trim().split("\\s*,\\s*");
            Collections.addAll(list, arr);
        }
        catch (Exception e) {}
        }

        return list;
    }

    // assigning grid cell ID - using coordinates
    /*
    public static List<String> assignGridCellID(Coordinate[] coordinates, UniformGrid uGrid) {

        List<String> gridCellIDs = new ArrayList<String>();

        for(Coordinate coordinate: coordinates) {

            // Direct approach to compute the cellIDs (Key)
            int xCellIndex = (int) (Math.floor((coordinate.getX() - uGrid.getMinX()) / uGrid.getCellLength()));
            int yCellIndex = (int) (Math.floor((coordinate.getY() - uGrid.getMinY()) / uGrid.getCellLength()));

            String gridIDStr = HelperClass.padLeadingZeroesToInt(xCellIndex, uGrid.getCellIndexStrLength()) + HelperClass.padLeadingZeroesToInt(yCellIndex, uGrid.getCellIndexStrLength());
            gridCellIDs.add(gridIDStr);
        }

        return gridCellIDs;
    }
     */

    public static org.locationtech.jts.geom.Polygon generatePolygonUsingBBox(Coordinate[] boundingBox) {

        // creating a cell polygon for the sake of computing the intersection between polygon and line segment
        if (boundingBox.length > 0) {

            Coordinate c1 = new Coordinate(boundingBox[0].getX(), boundingBox[0].getY(), 0);
            Coordinate c2 = new Coordinate(boundingBox[1].getX(), boundingBox[0].getY(), 0);
            Coordinate c3 = new Coordinate(boundingBox[1].getX(), boundingBox[1].getY(), 0);
            Coordinate c4 = new Coordinate(boundingBox[0].getX(), boundingBox[1].getY(), 0);

            Coordinate[] polygonCoordinates;
            polygonCoordinates = new Coordinate[]{c1, c2, c3, c4, c1};
            GeometryFactory geofact = new GeometryFactory();
            org.locationtech.jts.geom.Polygon poly = geofact.createPolygon(polygonCoordinates);

            return poly;
        }
        return null;
    }

    public static ArrayList<Integer> getIntCellIndices(String cellID)
    {
        ArrayList<Integer> cellIndices = new ArrayList<Integer>();

        //substring(int startIndex, int endIndex): endIndex is excluded
        //System.out.println("cellIndices.size() " + cellIndices.size());
        String cellIDX = cellID.substring(0,5);
        String cellIDY = cellID.substring(5);

        cellIndices.add(HelperClass.removeLeadingZeroesFromString(cellIDX));
        cellIndices.add(HelperClass.removeLeadingZeroesFromString(cellIDY));

        return cellIndices;
    }

    public static Integer getCellLayerWRTQueryCell(String queryCellID, String cellID)
    {
        ArrayList<Integer> queryCellIndices = getIntCellIndices(queryCellID);
        ArrayList<Integer> cellIndices = getIntCellIndices(cellID);
        Integer cellLayer;

        if((queryCellIndices.get(0).equals(cellIndices.get(0))) && (queryCellIndices.get(1).equals(cellIndices.get(1)))) {
            return 0; // cell layer is 0
        }
        else if ( Math.abs(queryCellIndices.get(0) - cellIndices.get(0)) == 0){
            return Math.abs(queryCellIndices.get(1) - cellIndices.get(1));
        }
        else if ( Math.abs(queryCellIndices.get(1) - cellIndices.get(1)) == 0){
            return Math.abs(queryCellIndices.get(0) - cellIndices.get(0));
        }
        else{
            return Math.max(Math.abs(queryCellIndices.get(0) - cellIndices.get(0)), Math.abs(queryCellIndices.get(1) - cellIndices.get(1)));
        }
    }

    // Generation of replicated polygon stream corresponding to each grid cell a polygon belongs
    public static class ReplicatePolygonStream extends RichFlatMapFunction<Polygon, Polygon> {

        private Integer parallelism;
        private Integer uniqueObjID;

        @Override
        public void open(Configuration parameters) {
            RuntimeContext ctx = getRuntimeContext();
            parallelism = ctx.getNumberOfParallelSubtasks();
            uniqueObjID = ctx.getIndexOfThisSubtask();
        }

        @Override
        public void flatMap(Polygon poly, Collector<Polygon> out) throws Exception {

            // Create duplicated polygon stream based on GridIDs
            for (String gridID: poly.gridIDsSet) {
                Polygon p = new Polygon(poly.getCoordinates(), Integer.toString(uniqueObjID), poly.gridIDsSet, gridID, poly.timeStampMillisec, poly.boundingBox);
                out.collect(p);
            }

            // Generating unique ID for each polygon, so that all the replicated tuples are assigned the same unique id by each parallel process
            uniqueObjID += parallelism;
        }
    }

    public static class ReplicatePolygonStreamUsingObjID extends RichFlatMapFunction<Polygon, Polygon> {
        @Override
        public void flatMap(Polygon poly, Collector<Polygon> out) throws Exception {

            // Create duplicated polygon stream based on GridIDs
            for (String gridID: poly.gridIDsSet) {
                Polygon p = new Polygon(poly.getCoordinates(), poly.objID, poly.gridIDsSet, gridID, poly.timeStampMillisec, poly.boundingBox);
                out.collect(p);
            }
        }
    }


    // Generation of replicated linestring stream corresponding to each grid cell a linestring belongs
    public static class ReplicateLineStringStream extends RichFlatMapFunction<LineString, LineString> {

        private long parallelism;
        private long uniqueObjID;

        @Override
        public void open(Configuration parameters) {
            RuntimeContext ctx = getRuntimeContext();
            parallelism = ctx.getNumberOfParallelSubtasks();
            uniqueObjID = ctx.getIndexOfThisSubtask();
        }

        @Override
        public void flatMap(LineString lineString, Collector<LineString> out) throws Exception {

            // Create duplicated polygon stream based on GridIDs
            for (String gridID: lineString.gridIDsSet) {
                LineString l = new LineString(String.valueOf(uniqueObjID), Arrays.asList(lineString.lineString.getCoordinates()), lineString.gridIDsSet, gridID, lineString.boundingBox);
                out.collect(l);
            }

            // Generating unique ID for each polygon, so that all the replicated tuples are assigned the same unique id
            uniqueObjID += parallelism;
        }
    }

    // Generation of replicated linestring stream corresponding to each grid cell a linestring belongs
    public static class ReplicateLineStringStreamUsingObjID extends RichFlatMapFunction<LineString, LineString> {
        @Override
        public void flatMap(LineString lineString, Collector<LineString> out) throws Exception {

            // Create duplicated polygon stream based on GridIDs
            for (String gridID: lineString.gridIDsSet) {
                LineString l = new LineString(lineString.objID, Arrays.asList(lineString.lineString.getCoordinates()), lineString.gridIDsSet, gridID, lineString.boundingBox);
                out.collect(l);
            }
        }
    }


    public static double computeHaverSine(Double lon, Double lat, Double lon1, Double lat1) {
        Double rLat1 = Math.toRadians(lat);
        Double rLat2 = Math.toRadians(lat1);
        Double dLon=Math.toRadians(lon1-lon);
        Double distance= Math.acos(Math.sin(rLat1)*Math.sin(rLat2) + Math.cos(rLat1)*Math.cos(rLat2) * Math.cos(dLon)) * mEarthRadius;
        return distance;
    }

    public static Set<Polygon> generateQueryPolygons(int numQueryPolygons, double minX, double minY, double maxX, double maxY, UniformGrid uGrid){

        // Generating random polygons for testing
        int gridSize = 100; // 25, 50, 100
        int numQPolygons = numQueryPolygons;
        Set<Polygon> qPolygonSet = new HashSet<>();

        double polyLength1 = (maxX - minX)/gridSize;
        double polyLength2 = (maxY - minY)/gridSize;
        double polyLength;
        if(polyLength2 < polyLength1)
            polyLength = polyLength2;
        else
            polyLength = polyLength1;

        for (double i = minX; i < maxX; i+= polyLength) {
            if(qPolygonSet.size() >= numQPolygons)
                break;

            for (double j = minY; j < maxY; j+= polyLength) {

                List<Coordinate> qPolygonCoordinates = new ArrayList<Coordinate>();

                qPolygonCoordinates.add(new Coordinate(i, j));
                qPolygonCoordinates.add(new Coordinate(i + polyLength, j));
                qPolygonCoordinates.add(new Coordinate(i + polyLength, j + polyLength));
                qPolygonCoordinates.add(new Coordinate(i, j + polyLength));
                qPolygonCoordinates.add(new Coordinate(i, j));

                List<List<Coordinate>> innerList = new ArrayList<List<Coordinate>>();
                innerList.add(qPolygonCoordinates);
                Polygon qPolygon = new Polygon(innerList, uGrid);
                qPolygonSet.add(qPolygon);
            }
        }
        /*
        for (int i = 0; i < numPolygons; i++) {
            List<Coordinate> qPolygonCoordinates = new ArrayList<Coordinate>();
            for (int j = 0; j < 4; j++) {
                double x = 115.5 + (Math.random() * (117.6 - 115.5));
                double y = 39.6 + (Math.random() * (41.1 - 39.6));
                qPolygonCoordinates.add(new Coordinate(x, y));
            }
            qPolygonCoordinates.add(qPolygonCoordinates.get(0));
            List<List<Coordinate>> innerList = new ArrayList<List<Coordinate>>();
            innerList.add(qPolygonCoordinates);
            Polygon qPolygon = new Polygon(innerList, uGrid);
            qPolygonSet.add(qPolygon);
        }
         */

        return qPolygonSet;
    }

    public static class checkExitControlTuple implements FilterFunction<ObjectNode> {
        @Override
        public boolean filter(ObjectNode json) throws Exception {
            String objType = json.get("value").get("geometry").get("type").asText();
            if (objType.equals("control")) {
                try {
                    throw new IOException();
                } finally {}

            }
            else return true;
        }
    }

    public static class LatencySinkPoint implements Serializable, KafkaSerializationSchema<Point> {

        String outputTopic;
        Integer queryID;

        public LatencySinkPoint(int queryID, String outputTopicName)
        {
            this.outputTopic = outputTopicName;
            this.queryID = queryID;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Point element, @Nullable Long timestamp) {
            Date date = new Date();
            Long latency =  date.getTime() - element.ingestionTime;
            //String outputStr = queryID.toString() + ", " + latency.toString();
            String outputStr = latency.toString();
            //System.out.println(latency);
            return new ProducerRecord<byte[], byte[]>(outputTopic, outputStr.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class LatencySinkLong implements Serializable, KafkaSerializationSchema<Long> {

        String outputTopic;

        public LatencySinkLong(String outputTopicName)
        {
            this.outputTopic = outputTopicName;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Long latency, @Nullable Long timestamp) {
            String outputStr = latency.toString();
            return new ProducerRecord<byte[], byte[]>(outputTopic, outputStr.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class LatencySinkTuple5 implements Serializable, KafkaSerializationSchema<Tuple5<String, Double, Long, Double, Long>> {

        String outputTopic;
        Integer queryID;

        public LatencySinkTuple5(int queryID, String outputTopicName)
        {
            this.outputTopic = outputTopicName;
            this.queryID = queryID;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Tuple5<String, Double, Long, Double, Long> element, @Nullable Long timestamp) {
            //String outputStr = queryID.toString() + ", " + element.f4.toString();
            String outputStr = element.f4.toString();
            return new ProducerRecord<byte[], byte[]>(outputTopic, outputStr.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class LatencySinkTuple4 implements Serializable, KafkaSerializationSchema<Tuple4<String, Integer, HashMap<String, Long>, Long>> {

        String outputTopic;
        Integer queryID;

        public LatencySinkTuple4(int queryID, String outputTopicName)
        {
            this.outputTopic = outputTopicName;
            this.queryID = queryID;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Tuple4<String, Integer, HashMap<String, Long>, Long> element, @Nullable Long timestamp) {
            //String outputStr = queryID.toString() + ", " + element.f3.toString();
            String outputStr = element.f3.toString();
            return new ProducerRecord<byte[], byte[]>(outputTopic, outputStr.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class cellBasedPolygonFlatMap implements FlatMapFunction<Polygon, Polygon> {

        Set<String> neighboringCells = new HashSet<String>();

        //ctor
        public cellBasedPolygonFlatMap() {}
        public cellBasedPolygonFlatMap(Set<String> neighboringCells) {
            this.neighboringCells = neighboringCells;
        }

        @Override
        public void flatMap(Polygon poly, Collector<Polygon> output) throws Exception {

            // If a polygon is either a CN or GN
            // Return 0 or 1 Polygon with gridID which is either a CN or GN
            Polygon outputPolygon;
            for(String gridID: poly.gridIDsSet) {
                if (neighboringCells.contains(gridID)) {
                    outputPolygon = new Polygon(poly.getCoordinates(), poly.objID, poly.gridIDsSet, poly.gridID, poly.timeStampMillisec, poly.boundingBox);
                    output.collect(outputPolygon);
                    return;
                }
            }
        }
    }

    public static class cellBasedLineStringFlatMap implements FlatMapFunction<LineString, LineString> {

        Set<String> neighboringCells = new HashSet<String>();

        //ctor
        public cellBasedLineStringFlatMap() {}
        public cellBasedLineStringFlatMap(Set<String> neighboringCells) {
            this.neighboringCells = neighboringCells;
        }

        @Override
        public void flatMap(LineString lineString, Collector<LineString> output) throws Exception {

            // If a polygon is either a CN or GN
            // Return 0 or 1 LineString with gridID which is either a CN or GN
            LineString outputLineString;
            for(String gridID: lineString.gridIDsSet) {
                if (neighboringCells.contains(gridID)) {
                    outputLineString = new LineString(
                            lineString.objID, new ArrayList<Coordinate>(Arrays.asList(lineString.lineString.getCoordinates())),
                            lineString.gridIDsSet, gridID, lineString.boundingBox);
                    output.collect(outputLineString);
                    return;
                }
            }
        }
    }





}