package GeoFlink.utils;

import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;

public class DistanceFunctions {

    // ctor
    public DistanceFunctions() {}

    // Point-to-Point Distance
    public static double getDistance(Point obj1, Point obj2)
    {
        return obj1.point.distance(obj2.point);
    }

    // Polygon-to-Polygon Distance
    public static double getDistance(Polygon obj1, Polygon obj2)
    {
        return obj1.polygon.distance(obj2.polygon);
    }

    // LineString-to-LineString Distance
    public static double getDistance(LineString obj1, LineString obj2)
    {
        return obj1.lineString.distance(obj2.lineString);
    }

    // Point-to-Polygon Distance
    public static double getDistance(Point obj1, Polygon obj2)
    {
        return obj1.point.distance(obj2.polygon);
    }

    // Point-to-LineString Distance
    public static double getDistance(Point obj1, LineString obj2)
    {
        return obj1.point.distance(obj2.lineString);
    }

    // Polygon-to-LineString Distance
    public static double getDistance(Polygon obj1, LineString obj2)
    {
        return obj1.polygon.distance(obj2.lineString);
    }

    // LineString-to-Polygon Distance
    public static double getDistance(LineString obj1, Polygon obj2)
    {
        return obj1.lineString.distance(obj2.polygon);
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


    // Get min distance between Polygon and LineString bounding box
    public static double getPolygonLineStringBBoxMinEuclideanDistance(Polygon poly, LineString lineString) {
        Tuple2<Coordinate, Coordinate> bBox1
                = new Tuple2<Coordinate, Coordinate>(
                new Coordinate(poly.boundingBox.f0.getX(), poly.boundingBox.f0.getY()),
                new Coordinate(poly.boundingBox.f1.getX(), poly.boundingBox.f1.getY()));
        Tuple2<Coordinate, Coordinate> bBox2
                = new Tuple2<Coordinate, Coordinate>(
                new Coordinate(lineString.boundingBox.f0.getX(), lineString.boundingBox.f0.getY()),
                new Coordinate(lineString.boundingBox.f1.getX(), lineString.boundingBox.f1.getY()));
        return getBBoxBBoxMinEuclideanDistance(bBox1, bBox2);
    }

    // Get min distance between Polygon and LineString bounding box
    public static double getLineStringLineStringBBoxMinEuclideanDistance(LineString srcLineString, LineString dstLineString) {
        Tuple2<Coordinate, Coordinate> bBox1
                = new Tuple2<Coordinate, Coordinate>(
                new Coordinate(srcLineString.boundingBox.f0.getX(), srcLineString.boundingBox.f0.getY()),
                new Coordinate(srcLineString.boundingBox.f1.getX(), srcLineString.boundingBox.f1.getY()));
        Tuple2<Coordinate, Coordinate> bBox2
                = new Tuple2<Coordinate, Coordinate>(
                new Coordinate(dstLineString.boundingBox.f0.getX(), dstLineString.boundingBox.f0.getY()),
                new Coordinate(dstLineString.boundingBox.f1.getX(), dstLineString.boundingBox.f1.getY()));
        return getBBoxBBoxMinEuclideanDistance(bBox1, bBox2);
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
     */


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





}
