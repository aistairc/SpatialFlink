package GeoFlink.utils;

import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

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

    /* TODO remove this block
    public static class inTupleDoubleDistanceComparator implements Comparator<Tuple2<String, Double>>, Serializable {

        public inTupleDoubleDistanceComparator() {}

        public int compare(Tuple2<String, Double> t1, Tuple2<String, Double> t2) {

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
     */

        public static class inTupleLineStringDistanceComparator implements Comparator<Tuple2<LineString, Double>>, Serializable {

        public inTupleLineStringDistanceComparator() {}

        @Override
        public int compare(Tuple2<LineString, Double> t1, Tuple2<LineString, Double> t2) {

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

    public class SpatialDistanceComparator implements Comparator<Tuple2<Point, Double>>, Serializable {

        Point queryPoint;

        public SpatialDistanceComparator() {
        }

        public SpatialDistanceComparator(Point queryPoint) {
            this.queryPoint = queryPoint;
        }

        public int compare(Tuple2<Point, Double> t1, Tuple2<Point, Double> t2) {
            // computeSpatialDistance(Double lon, Double lat, Double lon1, Double lat1)
            double distance1 = DistanceFunctions.getPointPointEuclideanDistance(t1.f0.point.getX(), t1.f0.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
            double distance2 = DistanceFunctions.getPointPointEuclideanDistance(t2.f0.point.getX(), t2.f0.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());

            if (distance1 > distance2) {
                return -1;
            } else if (distance1 == distance2) {
                return 0;
            }
            return 1;
        }
    }
}
