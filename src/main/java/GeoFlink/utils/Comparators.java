package GeoFlink.utils;

import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class Comparators {

    public static class inTuplePointDistanceComparator implements Comparator<Tuple2<Point, Double>>, Serializable {

        public inTuplePointDistanceComparator() {}

        public int compare(Tuple2<Point, Double> t1, Tuple2<Point, Double> t2) {
            //double distance1 = HelperClass.getPointPointEuclideanDistance(t1.f0.point.getX(), t1.f0.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
            //double distance2 = HelperClass.getPointPointEuclideanDistance(t2.f0.point.getX(), t2.f0.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
            double distance1 = t1.f1;
            double distance2 = t2.f1;

            // Maintains a PQ in descending order with the object with the largest distance from query point at the top
            if (distance1 > distance2) {
                return -1;
            } else if (distance1 == distance2) {
                return 0;
            }
            return 1;
        }
    }

    public static class inTuplePolygonDistanceComparator implements Comparator<Tuple2<Polygon, Double>>, Serializable {

        public inTuplePolygonDistanceComparator() {}

        public int compare(Tuple2<Polygon, Double> t1, Tuple2<Polygon, Double> t2) {

            double distance1 = t1.f1;
            double distance2 = t2.f1;

            // Maintains a PQ in descending order with the object with the largest distance from query point at the top
            if (distance1 > distance2) {
                return -1;
            } else if (distance1 == distance2) {
                return 0;
            }
            return 1;
        }
    }
}
