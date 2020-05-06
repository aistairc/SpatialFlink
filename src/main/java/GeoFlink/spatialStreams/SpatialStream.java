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

package GeoFlink.spatialStreams;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SpatialStream implements Serializable {


    public static DataStream<Point> PointStream(DataStream inputStream, String inputType, UniformGrid uGrid){

        DataStream<Point> pointStream = null;

        if(inputType.equals("GeoJSON")) {
            pointStream = inputStream.map(new GeoJSONToSpatial(uGrid));
        }
        else if (inputType.equals("CSV")){
            pointStream = inputStream.map(new CSVToSpatial(uGrid));
        }

        return pointStream;
    }

    public static class GeoJSONToSpatial extends RichMapFunction<ObjectNode, Point> {

        UniformGrid uGrid;

        //ctor
        public  GeoJSONToSpatial() {};

        public  GeoJSONToSpatial(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public Point map(ObjectNode json) throws Exception {

            String objType = json.get("value").get("geometry").get("type").asText();
            Point spatialPoint = new Point(json.get("value").get("geometry").get("coordinates").get(0).asDouble(), json.get("value").get("geometry").get("coordinates").get(1).asDouble(), uGrid);

            return spatialPoint;
        }
    }

    // Assuming that csv string contains objectID, timestamp, longitude, latitude at
    public static class CSVToSpatial extends RichMapFunction<ObjectNode, Point> {

        UniformGrid uGrid;

        //ctor
        public  CSVToSpatial() {};
        public  CSVToSpatial(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public Point map(ObjectNode strTuple) throws Exception {

            List<String> strArrayList = Arrays.asList(strTuple.toString().split("\\s*,\\s*"));

            Point spatialPoint = new Point(Double.parseDouble(strArrayList.get(0)), Double.parseDouble(strArrayList.get(1)), uGrid);

            return spatialPoint;
        }
    }

}


