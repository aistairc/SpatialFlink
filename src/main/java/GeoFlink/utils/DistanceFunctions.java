package GeoFlink.utils;

import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;

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
}
