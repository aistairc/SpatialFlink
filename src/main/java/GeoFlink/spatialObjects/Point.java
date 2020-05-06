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

package GeoFlink.spatialObjects;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class Point extends SpatialObject implements Serializable {
    public String gridID;
    public org.locationtech.jts.geom.Point point;

    public Point() {}; // required for POJO

    public Point(double x, double y, String gridID) {
        GeometryFactory geofact = new GeometryFactory();
        //create geotools point object
        point = geofact.createPoint(new Coordinate(x, y));
        this.gridID = gridID;
    }

    public Point(double x, double y, UniformGrid uGrid) {
        GeometryFactory geofact = new GeometryFactory();
        point = geofact.createPoint(new Coordinate(x, y));
        assignGridID(uGrid);
    }

    // To print the point coordinates
    @Override
    public String toString() {
        return "[" + point.getX() + ", " + point.getY() + ", " + this.gridID + "]";
    }


    //getters
    public static class getX implements MapFunction<Point, Double> {
        @Override
        public Double map(Point p) throws Exception {
            return p.point.getX();
        }
    }

    public static class getY implements MapFunction<Point, Double> {
        @Override
        public Double map(Point p) throws Exception {
            return p.point.getY();
        }
    }


    public static class getGridID implements MapFunction<Point, String> {
        @Override
        public String map(Point p) throws Exception {
            return p.gridID;

        }
    }

    // rolling grid-wise sum of spatial objects
    public static class addSummer implements MapFunction<Point, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(Point p) throws Exception {
            return Tuple2.of(p.gridID, 1);
        }
    }

    // max-min functions for coordinates
    public static class maxY implements ReduceFunction<Point> {
        @Override
        public Point reduce(Point p, Point p1) {
            if (p.point.getY() > p1.point.getY()) {
                return p;
            } else
                return p1;
        }
    }

    public static class minY implements ReduceFunction<Point> {
        @Override
        public Point reduce(Point p, Point p1) {
            if (p.point.getY() < p1.point.getY()) {
                return p;
            } else
                return p1;
        }
    }

    public static class maxX implements ReduceFunction<Point> {
        @Override
        public Point reduce(Point p, Point p1) {
            if (p.point.getX() > p1.point.getX()) {
                return p;
            } else
                return p1;
        }
    }

    public static class minX implements ReduceFunction<Point> {
        @Override
        public Point reduce(Point p, Point p1) {
            if (p.point.getX() < p1.point.getX()) {
                return p;
            } else
                return p1;
        }
    }

    public static class gridIDKeySelector implements KeySelector<Point,String> {
        @Override
        public String getKey(Point p) throws Exception {
            return p.gridID;
        }
    }


    private void assignGridID(UniformGrid uGrid) {

        // Direct approach to compute the cellIDs (Key)
        int xCellIndex = (int)(Math.floor((point.getX() - uGrid.getMinX())/uGrid.getCellLength()));
        int yCellIndex = (int)(Math.floor((point.getY() - uGrid.getMinY())/uGrid.getCellLength()));

        String gridIDStr = HelperClass.padLeadingZeroesToInt(xCellIndex, uGrid.getCellIndexStrLength()) + HelperClass.padLeadingZeroesToInt(yCellIndex, uGrid.getCellIndexStrLength());

        this.gridID = gridIDStr;
    }


    public static class distCompCounter extends RichMapFunction<Point, Point> {
        private transient Counter counter;

        @Override
        public void open(Configuration config) {
            this.counter = getRuntimeContext()
                    .getMetricGroup()
                    .counter("Distance Computation Count");
        }

        @Override
        public Point map(Point point) throws Exception {
            this.counter.inc();
            return point;
        }
    }

    public static class throughputMeterPoint extends RichMapFunction<Point, Point> {
        private transient Meter meter;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.meter = getRuntimeContext()
                    .getMetricGroup()
                    .meter("Throughput-Meter", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
        }

        @Override
        public Point map(Point point) throws Exception {
            this.meter.markEvent();
            return point;
        }
    }

    static class throughputMeterJoin extends RichMapFunction<Tuple4<Integer, String, List<Integer>, List<String>>, Object> {
        private transient Meter meter;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.meter = getRuntimeContext()
                    .getMetricGroup()
                    .meter("Throughput-Meter", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
        }

        @Override
        public Object map(Tuple4<Integer, String, List<Integer>, List<String>> value) throws Exception {
            this.meter.markEvent();
            return null;
        }
    }
}
