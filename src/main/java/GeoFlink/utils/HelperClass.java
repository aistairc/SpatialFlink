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
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.locationtech.jts.geom.Coordinate;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

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

    // assigning grid cell ID
    public static String assignGridCellID(Coordinate coordinate, UniformGrid uGrid) {

        // Direct approach to compute the cellIDs (Key)
        // int xCellIndex = (int)(Math.floor((point.getX() - uGrid.getMinX())/uGrid.getCellLength()));
        // int yCellIndex = (int)(Math.floor((point.getY() - uGrid.getMinY())/uGrid.getCellLength()));
        int xCellIndex = (int)(Math.floor((coordinate.getX() - uGrid.getMinX())/uGrid.getCellLength()));
        int yCellIndex = (int)(Math.floor((coordinate.getY() - uGrid.getMinY())/uGrid.getCellLength()));

        String gridIDStr = HelperClass.padLeadingZeroesToInt(xCellIndex, uGrid.getCellIndexStrLength()) + HelperClass.padLeadingZeroesToInt(yCellIndex, uGrid.getCellIndexStrLength());

        return gridIDStr;
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

        double minX = bBox.f0.getX();
        double minY = bBox.f0.getY();

        for(int x = xCellIndex1; x <= xCellIndex2; x++)
            for(int y = yCellIndex1; y <= yCellIndex2; y++)
            {
                String gridIDStr = HelperClass.padLeadingZeroesToInt(x, uGrid.getCellIndexStrLength()) + HelperClass.padLeadingZeroesToInt(y, uGrid.getCellIndexStrLength());
                gridCellIDs.add(gridIDStr);
            }

        return gridCellIDs;
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

    public static ArrayList<Integer> getIntCellIndices(String cellID)
    {
        ArrayList<Integer> cellIndices = new ArrayList<Integer>();

        //substring(int startIndex, int endIndex): endIndex is excluded
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

        if((queryCellIndices.get(0) == cellIndices.get(0)) && (queryCellIndices.get(1) == cellIndices.get(1))) {
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

    public static double getPointPointEuclideanDistance(Coordinate c1, Coordinate c2) {

        return getPointPointEuclideanDistance(c1.getX(), c1.getY(), c2.getX(), c2.getY());
    }

    public static double getPointPointEuclideanDistance(Double lon, Double lat, Double lon1, Double lat1) {

        return Math.sqrt( Math.pow((lat1 - lat),2) + Math.pow((lon1 - lon),2));
    }

    static double getPointLineStringNearestBBoxBorderMinEuclideanDistance(Coordinate p, Coordinate c1, Coordinate c2){
        return getPointLineStringNearestBBoxBorderMinEuclideanDistance(p.getX(), p.getY(), c1.getX(), c1.getY(), c2.getX(), c2.getY());
    }

    // Get exact min distance between Point and Polygon
    public static double getPointPolygonMinEuclideanDistance(Point p, Polygon poly) {
        return getPointCoordinatesArrayMinEuclideanDistance(p, poly.polygon.getCoordinates());
    }

    static double getPointCoordinatesArrayMinEuclideanDistance(Point p, Coordinate[] coordinates) {

        double minDist = Double.MAX_VALUE;
        for(int i = 0; i < coordinates.length - 1; i++){
            double dist = getPointLineSegmentMinEuclideanDistance(p.point.getCoordinate(), coordinates[i], coordinates[i+1]);

            if (dist < minDist){
                minDist = dist;
            }
        }
        return minDist;
    }

    // Get exact min distance between Point and LineString
    public static double getPointLineStringMinEuclideanDistance(Point p, LineString lineString) {
        return getPointCoordinatesArrayMinEuclideanDistance(p, lineString.lineString.getCoordinates());
    }

    // Exact Min distance between a point and a line segment of two points
    // Point Line-Segment Distance. Source: https://stackoverflow.com/questions/849211/shortest-distance-between-a-point-and-a-line-segment
    // x, y are point coordinates, whereas x1, y1 and x2, y2 are line segment co-ordinates

    public static double getPointLineSegmentMinEuclideanDistance(Coordinate pointCoordinate, Coordinate lineSegmentCoordinate1, Coordinate lineSegmentCoordinate2){
        return getPointLineSegmentMinEuclideanDistance(pointCoordinate.getX(), pointCoordinate.getY(),  lineSegmentCoordinate1.getX(), lineSegmentCoordinate1.getY(), lineSegmentCoordinate2.getX(), lineSegmentCoordinate2.getY());
    }

    public static double getPointLineSegmentMinEuclideanDistance(double x, double y, double x1, double y1, double x2, double y2){

        double A = x - x1;
        double B = y - y1;
        double C = x2 - x1;
        double D = y2 - y1;

        double dot = (A * C) + (B * D);
        double len_sq = (C * C) + (D * D);
        double param = -1;

        if (len_sq != 0) //in case of 0 length line
            param = dot / len_sq;

        double xx;
        double yy;

        if (param < 0) {
            xx = x1;
            yy = y1;
        }
        else if (param > 1) {
            xx = x2;
            yy = y2;
        }
        else {
            xx = x1 + param * C;
            yy = y1 + param * D;
        }

        return getPointPointEuclideanDistance(x, y, xx, yy);
    }

    public static double getPointLineStringNearestBBoxBorderMinEuclideanDistance(double x, double y, double x1, double y1, double x2, double y2){

        if(x1 == x2){
            return getPointPointEuclideanDistance(x, y, x1, y);
        }
        else if(y1 == y2){
            return getPointPointEuclideanDistance(x, y, x, y1);
        }
        else{
            System.out.println("getPointLineStringNearestBBoxBorderMinEuclideanDistance: invalid bbox coordinates");
        }

        return Double.MIN_VALUE;
    }

    public static String getSubstringFollowingGivenString(String str, String strToFind){
        return str.substring(str.indexOf(strToFind) + 1);
    }


    // Get min distance between Point and Polygon bounding box
    public static double getPointPolygonBBoxMinEuclideanDistance(Point p, Polygon poly) {

        // Point coordinates
        double x = p.point.getX();
        double y = p.point.getY();

        // Line coordinate 1
        double x1 = poly.boundingBox.f0.getX();
        double y1 = poly.boundingBox.f0.getY();

        // Line coordinate 2
        double x2 = poly.boundingBox.f1.getX();
        double y2 = poly.boundingBox.f1.getY();

        if(x <= x1){

            if(y <= y1){
                return getPointPointEuclideanDistance(x,y,x1,y1);
            }
            else if (y >= y2){
                return getPointPointEuclideanDistance(x,y,x1,y2);
            }
            else{ // y > y1 && y < y2
                return getPointLineStringNearestBBoxBorderMinEuclideanDistance(x, y, x1, y1, x1, y2);
            }
        }
        else if(x >= x2){

            if(y <= y1){
                return getPointPointEuclideanDistance(x,y,x2,y1);
            }
            else if (y >= y2){
                return getPointPointEuclideanDistance(x,y,x2,y2);
            }
            else{ // y > y1 && y < y2
                return getPointLineStringNearestBBoxBorderMinEuclideanDistance(x, y, x2, y1, x2, y2);
            }
        }
        else{ // x > x1 && x < x2

            if(y <= y1){
                return getPointLineStringNearestBBoxBorderMinEuclideanDistance(x, y, x1, y1, x2, y1);
            }
            else if (y >= y2){
                return getPointLineStringNearestBBoxBorderMinEuclideanDistance(x, y, x1, y2, x2, y2);
            }
            else{ // y > y1 && y < y2
                return 0.0; // Query point is within bounding box or on the boundary
            }
        }
    }


    // Get min distance between Point and LineString bounding box
    public static double getPointLineStringBBoxMinEuclideanDistance(Point p, LineString lineString) {

        // Point coordinates
        double x = p.point.getX();
        double y = p.point.getY();

        // Line coordinate 1
        double x1 = lineString.boundingBox.f0.getX();
        double y1 = lineString.boundingBox.f0.getY();

        // Line coordinate 2
        double x2 = lineString.boundingBox.f1.getX();
        double y2 = lineString.boundingBox.f1.getY();

        // identify the nearest bounding box boundary and compute distance from point
        if(x <= x1){

            if(y <= y1){
                return getPointPointEuclideanDistance(x,y,x1,y1);
            }
            else if (y >= y2){
                return getPointPointEuclideanDistance(x,y,x1,y2);
            }
            else{ // y > y1 && y < y2
                return getPointLineStringNearestBBoxBorderMinEuclideanDistance(x, y, x1, y1, x1, y2);
            }
        }
        else if(x >= x2){

            if(y <= y1){
                return getPointPointEuclideanDistance(x,y,x2,y1);
            }
            else if (y >= y2){
                return getPointPointEuclideanDistance(x,y,x2,y2);
            }
            else{ // y > y1 && y < y2
                return getPointLineStringNearestBBoxBorderMinEuclideanDistance(x, y, x2, y1, x2, y2);
            }
        }
        else{ // x > x1 && x < x2

            if(y <= y1){
                return getPointLineStringNearestBBoxBorderMinEuclideanDistance(x, y, x1, y1, x2, y1);
            }
            else if (y >= y2){
                return getPointLineStringNearestBBoxBorderMinEuclideanDistance(x, y, x1, y2, x2, y2);
            }
            else{ // y > y1 && y < y2
                return 0.0; // Query point is within bounding box or on the boundary
            }
        }
    }


    // Get min distance between Polygon and Polygon bounding box
    public static double getPolygonPolygonBBoxMinEuclideanDistance(Polygon srcPoly, Polygon dstPoly) {
        Tuple2<Coordinate, Coordinate> bBox1
                = new Tuple2<Coordinate, Coordinate>(
                        new Coordinate(srcPoly.boundingBox.f0.getX(), srcPoly.boundingBox.f0.getY()),
                        new Coordinate(srcPoly.boundingBox.f1.getX(), srcPoly.boundingBox.f1.getY()));
        Tuple2<Coordinate, Coordinate> bBox2
                = new Tuple2<Coordinate, Coordinate>(
                        new Coordinate(dstPoly.boundingBox.f0.getX(), dstPoly.boundingBox.f0.getY()),
                        new Coordinate(dstPoly.boundingBox.f1.getX(), dstPoly.boundingBox.f1.getY()));
        return getBBoxBBoxMinEuclideanDistance(bBox1, bBox2);
    }


    // check the overlapping of 2 rectangles
    static boolean doRectanglesOverlap(Coordinate bottomLeft1, Coordinate topRight1, Coordinate bottomLeft2, Coordinate topRight2) {
        // If one rectangle is on left side of other
        if (bottomLeft1.getX() >= topRight2.getX() || bottomLeft2.getX() >= topRight1.getX()) {
            return false;
        }

        // If one rectangle is above other
        if (topRight1.getY() <= bottomLeft2.getY() || topRight2.getY() <= bottomLeft1.getY()) {
            return false;
        }

        return true;
    }


    public static double getBBoxBBoxMinEuclideanDistance(Tuple2<Coordinate, Coordinate> bBox1, Tuple2<Coordinate, Coordinate> bBox2) {

        // Polygon coordinate 1
        //Tuple2<Coordinate, Coordinate> x = poly1.boundingBox;

        double x1 = bBox1.f0.getX();
        double y1 = bBox1.f0.getY();
        double x2 = bBox1.f1.getX();
        double y2 = bBox1.f1.getY();

        Coordinate a1 = new Coordinate(x1, y1);
        Coordinate a2 = new Coordinate(x2, y1);
        Coordinate a3 = new Coordinate(x2, y2);
        Coordinate a4 = new Coordinate(x1, y2);

        // Polygon coordinate 2
        double p1 = bBox2.f0.getX();
        double q1 = bBox2.f0.getY();
        double p2 = bBox2.f1.getX();
        double q2 = bBox2.f1.getY();

        Coordinate b1 = new Coordinate(p1, q1);
        Coordinate b2 = new Coordinate(p2, q1);
        Coordinate b3 = new Coordinate(p2, q2);
        Coordinate b4 = new Coordinate(p1, q2);


        // Compute the min. distance between two polygons
        if (p2 <= x1) {

            if(q2 <= y1) {
                return getPointPointEuclideanDistance(a1, b3);
            }
            else if(q1 >= y2){
                return getPointPointEuclideanDistance(a4, b2);
            }
            else{
                return getPointLineStringNearestBBoxBorderMinEuclideanDistance(b2, a1, a4);
            }

        } else if (p1 >= x2) {

            if(q2 <= y1) {
                return getPointPointEuclideanDistance(a2, b4);
            }
            else if(q1 >= y2){
                return getPointPointEuclideanDistance(a3, b1);
            }
            else{ // (q1 <= y2 && q2 >= y2 )
                return getPointLineStringNearestBBoxBorderMinEuclideanDistance(b1, a2, a3);
            }

        } else { // p2 > x1 && p1 < x2

            if(q2 <= y1) {
                return getPointLineStringNearestBBoxBorderMinEuclideanDistance(b4, a1, a2);
            }
            else if(q1 >= y2){ // q1 >= y2
                return getPointLineStringNearestBBoxBorderMinEuclideanDistance(b1, a3, a4);
            }
            else{
                return 0.0; // The 2 polygons overlap
            }
        }

        /*
        //Check if the 2 bounding boxes overlap
        if(doRectanglesOverlap(a1, a3, b1, b3)) {
            return 0; // if the 2 rectangles overlap, return 0
        }
        else{  // if the 2 rectangles do not overlap, return the min. distance between them

            if (p2 <= x1) {

                if(q2 <= y1) {
                    return getPointPointEuclideanDistance(a1, b3);
                }
                else if(q1 >= y2){
                    return getPointPointEuclideanDistance(a4, b2);
                }
                else if( (q2 >= y1 && q1 <= y1) || (q2 <= y2 && q1 >= y1) ){
                    return getPointLineStringNearestBBoxBorderMinEuclideanDistance(b3, a1, a4);
                }
                else{ // (q1 <= y2 && q2 >= y2 )
                    return getPointLineStringNearestBBoxBorderMinEuclideanDistance(b2, a1, a4);
                }

            } else if (p1 >= x2) {

                if(q2 <= y1) {
                    return getPointPointEuclideanDistance(a2, b4);
                }
                else if(q1 >= y2){
                    return getPointPointEuclideanDistance(a3, b1);
                }
                else if( (q2 >= y1 && q1 <= y1) || (q2 <= y2 && q1 >= y1) ){
                    return getPointLineStringNearestBBoxBorderMinEuclideanDistance(b4, a2, a3);
                }
                else{ // (q1 <= y2 && q2 >= y2 )
                    return getPointLineStringNearestBBoxBorderMinEuclideanDistance(b1, a2, a3);
                }

            } else { // p2 > x1 && p1 < x2

                if(q2 <= y1) {

                    if( (p2 >= x1 && p1 <= x1) || (p2 <= x2 && p1 >= x1) )  {
                        return getPointLineStringNearestBBoxBorderMinEuclideanDistance(b3, a1, a2);
                    } else{ // (p2 >= x2 && p1 <= x2
                        return getPointLineStringNearestBBoxBorderMinEuclideanDistance(b4, a1, a2);
                    }

                }else{ // q1 >= y2

                    if( (p2 >= x1 && p1 <= x1) || (p2 <= x2 && p1 >= x1) )  {
                        return getPointLineStringNearestBBoxBorderMinEuclideanDistance(b2, a3, a4);
                    } else{ // (p2 >= x2 && p1 <= x2
                        return getPointLineStringNearestBBoxBorderMinEuclideanDistance(b1, a3, a4);
                    }
                }
            }
        }*/

    }


    /*
    // Get min distance between Polygon and Polygon
    public static double getPolygonPolygonBBoxMinEuclideanDistance(Polygon poly1, Polygon poly2) {

        return getBBoxBBoxMinEuclideanDistance(poly1.boundingBox, poly2.boundingBox);
    }

    // Get min distance between Polygon and LineString
    public static double getPolygonLineStringBBoxMinEuclideanDistance(Polygon poly, LineString lineString) {
        return getBBoxBBoxMinEuclideanDistance(poly.boundingBox, lineString.boundingBox);
    }

     */

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
                Polygon p = new Polygon(poly.getCoordinates(), Integer.toString(uniqueObjID), poly.gridIDsSet, gridID, poly.boundingBox);
                out.collect(p);
            }

            // Generating unique ID for each polygon, so that all the replicated tuples are assigned the same unique id
            uniqueObjID += parallelism;
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


    public static double computeHaverSine(Double lon, Double lat, Double lon1, Double lat1) {
        Double rLat1 = Math.toRadians(lat);
        Double rLat2 = Math.toRadians(lat1);
        Double dLon=Math.toRadians(lon1-lon);
        Double distance= Math.acos(Math.sin(rLat1)*Math.sin(rLat2) + Math.cos(rLat1)*Math.cos(rLat2) * Math.cos(dLon)) * mEarthRadius;
        return distance;
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
            Polygon outputPolygon;
            for(String gridID: poly.gridIDsSet) {
                if (neighboringCells.contains(gridID)) {
                    outputPolygon = new Polygon(poly.getCoordinates(), poly.objID, poly.gridIDsSet, gridID, poly.boundingBox);
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