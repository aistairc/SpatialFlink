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

import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Comparator;




// TODO: Obsolete class
    public class SpatialDistanceComparator implements Comparator<Tuple2<Point, Double>>, Serializable {


        Point queryPoint;

        public SpatialDistanceComparator() {
        }

        public SpatialDistanceComparator(Point queryPoint) {
            this.queryPoint = queryPoint;
        }

        public int compare(Tuple2<Point, Double> t1, Tuple2<Point, Double> t2) {
            // computeSpatialDistance(Double lon, Double lat, Double lon1, Double lat1)
            double distance1 = HelperClass.getPointPointEuclideanDistance(t1.f0.point.getX(), t1.f0.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
            double distance2 = HelperClass.getPointPointEuclideanDistance(t2.f0.point.getX(), t2.f0.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());

            if (distance1 > distance2) {
                return -1;
            } else if (distance1 == distance2) {
                return 0;
            }
            return 1;
        }
    }
