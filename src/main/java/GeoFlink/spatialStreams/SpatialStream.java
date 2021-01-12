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
import GeoFlink.spatialObjects.Polygon;
import com.typesafe.config.ConfigException;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.json.JSONArray;
import org.json.JSONObject;
import org.locationtech.jts.geom.Coordinate;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

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

    public static DataStream<Point> TrajectoryStream(DataStream inputStream, String inputType, DateFormat dateFormat, UniformGrid uGrid){

        DataStream<Point> trajectoryStream = null;

        if(inputType.equals("GeoJSON")) {
            trajectoryStream = inputStream.map(new GeoJSONToTSpatial(uGrid, dateFormat));
        }
        else if (inputType.equals("CSV")){
            trajectoryStream = inputStream.map(new CSVToTSpatial(uGrid, dateFormat));
        }
        // TODO Delete DEIM Checkin
        /*
        else if (inputType.equals("JSON")){
            trajectoryStream = inputStream.map(new JSONToTSpatial(uGrid, dateFormat));
        }*/

        return trajectoryStream;
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
        public Point map(ObjectNode jsonObj) throws Exception {

            Point spatialPoint = new Point(jsonObj.get("value").get("geometry").get("coordinates").get(0).asDouble(), jsonObj.get("value").get("geometry").get("coordinates").get(1).asDouble(), uGrid);
            return spatialPoint;
        }
    }

    public static class GeoJSONToTSpatial extends RichMapFunction<ObjectNode, Point> {

        UniformGrid uGrid;
        DateFormat dateFormat;

        //ctor
        public  GeoJSONToTSpatial() {};
        public  GeoJSONToTSpatial(UniformGrid uGrid, DateFormat dateFormat)
        {

            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
        };

        @Override
        public Point map(ObjectNode jsonObj) throws Exception {

            Point spatialPoint;
            Date date = new Date();


            if (this.dateFormat == null) {
                spatialPoint = new Point(jsonObj.get("value").get("properties").get("oID").asText(), jsonObj.get("value").get("geometry").get("coordinates").get(0).asDouble(), jsonObj.get("value").get("geometry").get("coordinates").get(1).asDouble(), jsonObj.get("value").get("properties").get("timestamp").asLong(), uGrid);
                //spatialPoint = new Point(jsonObj.get("value").get("properties").get("oID").asText(), jsonObj.get("value").get("geometry").get("coordinates").get(0).asDouble(), jsonObj.get("value").get("geometry").get("coordinates").get(1).asDouble(), date.getTime(), uGrid);
            }
            else {
                Date dateTime = this.dateFormat.parse(jsonObj.get("value").get("properties").get("timestamp").asText());
                long timeStampMillisec = dateTime.getTime();
                spatialPoint = new Point(jsonObj.get("value").get("properties").get("oID").asText(), jsonObj.get("value").get("geometry").get("coordinates").get(0).asDouble(), jsonObj.get("value").get("geometry").get("coordinates").get(1).asDouble(), timeStampMillisec, uGrid);
            }

            return spatialPoint;
        }
    }


    // Assuming that csv string contains longitude and latitude at positions 0 and 1, respectively
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

    // Assuming that csv string contains longitude and latitude at positions 0 and 1, respectively
    public static class CSVToTSpatial extends RichMapFunction<ObjectNode, Point> {

        UniformGrid uGrid;
        DateFormat dateFormat;

        //ctor
        public  CSVToTSpatial() {};
        public  CSVToTSpatial(UniformGrid uGrid, DateFormat dateFormat)
        {

            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
        };

        @Override
        public Point map(ObjectNode strTuple) throws Exception {

            // customized for ATC Shopping mall data
            //A sample tuple/record: 1351039728.980,9471001,-22366,2452,1261.421,780.711,-2.415,-2.441
            // time [ms] (unixtime + milliseconds/1000), person id, position x [mm], position y [mm], position z (height) [mm], velocity [mm/s], angle of motion [rad], facing angle [rad]

            Point spatialPoint;
            Date date = new Date();
            List<String> strArrayList = Arrays.asList(strTuple.toString().split("\\s*,\\s*")); // For parsing CSV with , followed by space
            //List<String> strArrayList = Arrays.asList(strTuple.toString().split(","));

            if (this.dateFormat == null) {
                Long timeStampMillisec = Long.parseLong(strArrayList.get(0)) * 1000;
                spatialPoint = new Point(strArrayList.get(1), Double.parseDouble(strArrayList.get(2)), Double.parseDouble(strArrayList.get(3)), timeStampMillisec, uGrid);
            }
            else {
                Date dateTime = this.dateFormat.parse(strArrayList.get(0));
                long timeStampMillisec = dateTime.getTime();
                spatialPoint = new Point(strArrayList.get(1), Double.parseDouble(strArrayList.get(2)), Double.parseDouble(strArrayList.get(3)), timeStampMillisec, uGrid);
            }



            return spatialPoint;
        }
    }

    // TODO Delete DEIM Checkin
    // DEIM 2021 App
    /*
    public static class JSONToTSpatial extends RichMapFunction<ObjectNode, Point> {

        UniformGrid uGrid;
        DateFormat dateFormat;

        //ctor
        public  JSONToTSpatial() {};
        public  JSONToTSpatial(UniformGrid uGrid, DateFormat dateFormat)
        {

            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
        }

        @Override
        public Point map(ObjectNode jsonObj) throws Exception {

            Point spatialPoint;
            Date date = new Date();

            //{"event_id": "DEIM2021", "device_id": "B-out", "user_id": "uid134", "timestamp": "12/25/2020 17:24:36 +0900", "coordinate": {"longitude": 117.1273981297417, "latitude": 39.75259928616479}}

            Date dateTime = this.dateFormat.parse(jsonObj.get("value").get("timestamp").asText());
            long timeStampMillisec = dateTime.getTime();
            spatialPoint = new Point(jsonObj.get("value").get("event_id").asText(), jsonObj.get("value").get("device_id").asText(), jsonObj.get("value").get("user_id").asText(), timeStampMillisec, jsonObj.get("value").get("coordinate").get("longitude").asDouble(), jsonObj.get("value").get("coordinate").get("latitude").asDouble());

            return spatialPoint;
        }
    }

     */


    public static DataStream<Polygon> PolygonStream(DataStream inputStream, String inputType, UniformGrid uGrid){

        DataStream<Polygon> polygonStream = null;

        if(inputType.equals("GeoJSON")) {
            polygonStream = inputStream.map(new GeoJSONToSpatialPolygon(uGrid)).startNewChain();
        }
        else if (inputType.equals("CSV")){
            polygonStream = inputStream.map(new CSVToSpatialPolygon(uGrid));
        }

        return polygonStream;
    }

    public static class GeoJSONToSpatialPolygon extends RichMapFunction<ObjectNode, Polygon> {

        UniformGrid uGrid;

        //ctor
        public  GeoJSONToSpatialPolygon() {};
        public  GeoJSONToSpatialPolygon(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        //58> {"key":368387,"value":{"geometry":{"coordinates":[[[[-73.797919,40.681402],[-73.797885,40.681331],[-73.798032,40.681289],[-73.798048,40.681285],[-73.798067,40.681324],[-73.798075,40.681322],[-73.798092,40.681357],[-73.79806,40.681366],[-73.798058,40.681363],[-73.79801,40.681376],[-73.797919,40.681402]]]],"type":"MultiPolygon"},"properties":{"base_bbl":"4119880033","bin":"4259746","cnstrct_yr":"1955","doitt_id":"527355","feat_code":"2100","geomsource":"Photogramm","groundelev":"26","heightroof":"26.82","lstmoddate":"2017-08-22T00:00:00.000Z","lststatype":"Constructed","mpluto_bbl":"4119880033","name":null,"shape_area":"1375.27323008172","shape_len":"159.1112668769"},"type":"Feature"}}


        @Override
        public Polygon map(ObjectNode jsonObj) throws Exception {

            List<Coordinate> coordinates = new ArrayList<>();
            JsonNode JSONCoordinatesArray;
            //{"geometry": {"coordinates": [[[[-73.980455, 40.661994], [-73.980542, 40.661889], [-73.980559, 40.661897], [-73.98057, 40.661885], [-73.980611, 40.661904], [-73.9806, 40.661917], [-73.980513, 40.662022], [-73.980455, 40.661994]]]], "type": "MultiPolygon"}, "properties": {"base_bbl": "3011030028", "bin": "3026604", "cnstrct_yr": "1892", "doitt_id": "33583", "feat_code": "2100", "geomsource": "Photogramm", "groundelev": "153", "heightroof": "31.65", "lstmoddate": "2020-01-28T00:00:00.000Z", "lststatype": "Constructed", "mpluto_bbl": "3011030028", "name": null, "shape_area": "926.10935740605", "shape_len": "139.11922551796"}, "type": "Feature"}

            // Differentiate Polygon and MultiPolygon
            if(jsonObj.get("value").get("geometry").get("type").asText().equalsIgnoreCase("MultiPolygon")) {
                JSONCoordinatesArray = jsonObj.get("value").get("geometry").get("coordinates").get(0).get(0);
            }
            else if (jsonObj.get("value").get("geometry").get("type").asText().equalsIgnoreCase("Polygon")){ // Polygon case??
                System.out.println(jsonObj.get("value").get("geometry").get("type").asText());
                JSONCoordinatesArray = jsonObj.get("value").get("geometry").get("coordinates").get(0);
            }
            else { // Point case ??
                System.out.println("Not polygon but: " + jsonObj.get("value").get("geometry").get("type").asText());
                JSONCoordinatesArray = jsonObj.get("value").get("geometry").get("coordinates").get(0);
            }

            if (JSONCoordinatesArray.isArray()) {
                for (final JsonNode JSONCoordinate : JSONCoordinatesArray) {
                    //Coordinate(latitude, longitude)
                    coordinates.add(new Coordinate(JSONCoordinate.get(0).asDouble(), JSONCoordinate.get(1).asDouble()));
                }
            }

            Polygon spatialPolygon = new Polygon(coordinates, uGrid);
            return spatialPolygon;
        }
    }

    // Assuming that csv string contains longitude and latitude at positions 0 and 1, respectively
    public static class CSVToSpatialPolygon extends RichMapFunction<ObjectNode, Polygon> {

        UniformGrid uGrid;

        //ctor
        public  CSVToSpatialPolygon() {};
        public  CSVToSpatialPolygon(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public Polygon map(ObjectNode strTuple) throws Exception {

            List<String> strArrayList = Arrays.asList(strTuple.toString().split("\\s*,\\s*"));
            //Polygon spatialPolygon = new Point(Double.parseDouble(strArrayList.get(0)), Double.parseDouble(strArrayList.get(1)), uGrid);
            //return spatialPolygon;

            return null;
        }
    }

}