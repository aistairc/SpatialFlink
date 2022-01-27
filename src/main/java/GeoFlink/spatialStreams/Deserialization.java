/*
Copyright 2021 Data Platform Research Team, AIRC, AIST, Japan

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
import GeoFlink.spatialObjects.*;
import org.locationtech.jts.geom.Geometry;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.locationtech.jts.geom.Coordinate;
//import org.locationtech.jts.geom.Geometry;
//import org.wololo.geojson.Feature;
//import org.wololo.geojson.GeoJSONFactory;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.locationtech.jts.geom.GeometryFactory;


import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Deserialization implements Serializable {

    private static GeoJsonReader geoJsonReader = new GeoJsonReader(new GeometryFactory());

    public static DataStream<Point> PointStream(DataStream inputStream, String inputType, String delimiter, List<Integer> csvTsvSchemaAttr, UniformGrid uGrid){

        DataStream<Point> pointStream = null;

        if(inputType.equals("GeoJSON")) {
            pointStream = inputStream.map(new GeoJSONToSpatial(uGrid));
        }
        else if(inputType.equals("WKT")) {
            pointStream = inputStream.map(new WKTToSpatial(uGrid)).returns(Point.class);
        }
        else {
            pointStream = inputStream.map(new CSVTSVToSpatial(uGrid, delimiter, csvTsvSchemaAttr)).returns(Point.class);
        }

        return pointStream;
    }

    public static DataStream<Point> TrajectoryStream(DataStream inputStream, String inputType, DateFormat dateFormat, String delimiter,
                                                     List<Integer> csvTsvSchemaAttr, String propertyTimeStamp, String propertyObjID, UniformGrid uGrid){

        DataStream<Point> trajectoryStream = null;

        if(inputType.equals("GeoJSON")) {
            trajectoryStream = inputStream.map(new GeoJSONToTSpatial(uGrid, dateFormat, propertyTimeStamp, propertyObjID));
        }
        else if(inputType.equals("WKT")) {
            trajectoryStream = inputStream.map(new WKTToTSpatial(uGrid, dateFormat, delimiter)).returns(Point.class);
        }
        else {
            trajectoryStream = inputStream.map(new CSVTSVToTSpatial(uGrid, dateFormat, delimiter, csvTsvSchemaAttr)).returns(Point.class);
        }

        return trajectoryStream;
    }

    public static DataStream<Polygon> PolygonStream(DataStream inputStream, String inputType, UniformGrid uGrid){

        DataStream<Polygon> polygonStream = null;

        if(inputType.equals("GeoJSON")) {
            polygonStream = inputStream.map(new GeoJSONToSpatialPolygon(uGrid));
        }
        else if(inputType.equals("WKT")) {
            polygonStream = inputStream.map(new WKTToSpatialPolygon(uGrid)).returns(Polygon.class);
        }
        else {
            throw new IllegalArgumentException("inputType : " + inputType + " is not support");
        }

        return polygonStream;
    }

    public static DataStream<Polygon> TrajectoryStreamPolygon(DataStream inputStream, String inputType, DateFormat dateFormat, String delimiter,
                                                              String propertyTimeStamp, String propertyObjID, UniformGrid uGrid){

        DataStream<Polygon> trajectoryStream = null;

        if(inputType.equals("GeoJSON")) {
            trajectoryStream = inputStream.map(new GeoJSONToTSpatialPolygon(uGrid, dateFormat, propertyTimeStamp, propertyObjID));
        }
        else if(inputType.equals("WKT")) {
            trajectoryStream = inputStream.map(new WKTToTSpatialPolygon(uGrid, dateFormat, delimiter)).returns(Point.class);
        }
        else {
            throw new IllegalArgumentException("inputType : " + inputType + " is not support");
        }

        return trajectoryStream;
    }


    public static class GeoJSONToSpatial extends RichMapFunction<ObjectNode, Point> {

        // {"key":136138,"value":{"geometry":{"coordinates":[116.44412,39.93984],"type":"Point"},"properties":{"oID":"2560","timestamp":"2008-02-02 20:12:32"},"type":"Feature"}}
        // --> [ObjID: null, 116.56444, 40.07079, 0001200005, 0, 1611022449423]
        UniformGrid uGrid;

        //ctor
        public  GeoJSONToSpatial() {};
        public  GeoJSONToSpatial(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public Point map(ObjectNode jsonObj) throws Exception {

            Geometry geometry;
            try {
                geometry = readGeoJSON(jsonObj.get("value").toString());
            }
            catch (Exception e) {
                // "type" が無いStringの場合はGeometryを抽出する
                String jsonGeometry = jsonObj.get("value").get("geometry").toString();
                geometry = readGeoJSON(jsonGeometry);
            }

            Point spatialPoint = new Point(geometry.getCoordinate().x, geometry.getCoordinate().y, uGrid);
            return spatialPoint;
        }
    }

    public static class GeoJSONToTSpatial extends RichMapFunction<ObjectNode, Point> {

        UniformGrid uGrid;
        DateFormat dateFormat;
        String propertyTimeStamp;
        String propertyObjID;

        //ctor
        public  GeoJSONToTSpatial() {};
        public  GeoJSONToTSpatial(UniformGrid uGrid, DateFormat dateFormat, String propertyTimeStamp, String propertyObjID)
        {

            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
            this.propertyTimeStamp = propertyTimeStamp;
            this.propertyObjID = propertyObjID;
        };

        @Override
        public Point map(ObjectNode jsonObj) throws Exception {

            Geometry geometry;
            try {
                geometry = readGeoJSON(jsonObj.get("value").toString());
            }
            catch (Exception e) {
                // "type" が無いStringの場合はGeometryを抽出する
                String jsonGeometry = jsonObj.get("value").get("geometry").toString();
                geometry = readGeoJSON(jsonGeometry);
            }

            JsonNode nodeProperties = jsonObj.get("value").get("properties");
            String strOId = null;
            long time = 0;
            if (nodeProperties != null) {
                JsonNode nodeTime = jsonObj.get("value").get("properties").get(propertyTimeStamp);
                try {
                    if (nodeTime != null && dateFormat != null) {
                        time = dateFormat.parse(nodeTime.textValue()).getTime();
                    }
                    else if(nodeTime != null){
                        time = Long.parseLong(String.valueOf(nodeTime));
                    }
                }
                catch (ParseException e) {}
                JsonNode nodeOId = jsonObj.get("value").get("properties").get(propertyObjID);
                if (nodeOId != null) {
                    //strOId = nodeOId.textValue();
                    strOId = nodeOId.toString().replaceAll("\\\"", "");

                }
            }
            Point spatialPoint;
            if (time != 0) {
                spatialPoint = new Point(strOId, geometry.getCoordinate().x, geometry.getCoordinate().y, time, uGrid);
                //spatialPoint = new Point(strOId, geometry.getCoordinate().x, geometry.getCoordinate().y, System.currentTimeMillis(), uGrid);
            }
            else {
                spatialPoint = new Point(strOId, geometry.getCoordinate().x, geometry.getCoordinate().y, 0, uGrid);
            }
            return spatialPoint;
        }
    }

    // Assuming that csv string contains longitude and latitude at positions 0 and 1, respectively
    public static class WKTToSpatial extends RichMapFunction<String, Point> {

        UniformGrid uGrid;

        //ctor
        public WKTToSpatial() {};
        public WKTToSpatial(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public Point map(String str) throws Exception {
            int pos = str.indexOf("POINT");
            Coordinate coordinate = getCoordinate(str.substring(pos));
            Point spatialPoint = new Point(coordinate.x,  coordinate.y, uGrid);
            return spatialPoint;
        }
    }

    // Assuming that csv/tsv string contains longitude and latitude at positions 0 and 1, respectively
    public static class CSVTSVToSpatial extends RichMapFunction<String, Point> {

        UniformGrid uGrid;
        String delimiter;
        List<Integer> csvTsvSchemaAttr;

        //ctor
        public CSVTSVToSpatial() {};
        public CSVTSVToSpatial(UniformGrid uGrid, String delimiter, List<Integer> csvTsvSchemaAttr)
        {
            this.uGrid = uGrid;
            this.delimiter = delimiter;
            this.csvTsvSchemaAttr = csvTsvSchemaAttr;
        };

        @Override
        public Point map(String str) throws Exception {
            List<String> strArrayList = Arrays.asList(str.replace("\"", "").split("\\s*" + delimiter + "\\s*")); // For parsing CSV with , followed by space
            double x = Double.valueOf(strArrayList.get(csvTsvSchemaAttr.get(2)));
            double y = Double.valueOf(strArrayList.get(csvTsvSchemaAttr.get(3)));;
            Point spatialPoint = new Point(x, y, uGrid);
            return spatialPoint;
        }
    }

    // Assuming that csv string contains longitude and latitude at positions 0 and 1, respectively
    public static class WKTToTSpatial extends RichMapFunction<String, Point> {

        UniformGrid uGrid;
        DateFormat dateFormat;
        String delimiter;

        //ctor
        public WKTToTSpatial() {};
        public WKTToTSpatial(UniformGrid uGrid, DateFormat dateFormat, String delimiter)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
            this.delimiter = delimiter;
        };

        @Override
        public Point map(String str) throws Exception {

            // customized for ATC Shopping mall data
            //A sample tuple/record: 1351039728.980,9471001,-22366,2452,1261.421,780.711,-2.415,-2.441
            // time [ms] (unixtime + milliseconds/1000), person id, position x [mm], position y [mm], position z (height) [mm], velocity [mm/s], angle of motion [rad], facing angle [rad]

            int pos = str.indexOf("POINT");
            Coordinate coordinate = getCoordinate(str.substring(pos));
            Point spatialPoint = new Point(coordinate.x,  coordinate.y, uGrid);
            return spatialPoint;
        }
    }

    // Assuming that csv/tsv string contains longitude and latitude at positions 0 and 1, respectively
    public static class CSVTSVToTSpatial extends RichMapFunction<String, Point> {

        UniformGrid uGrid;
        DateFormat dateFormat;
        String delimiter;
        List<Integer> csvTsvSchemaAttr;

        //ctor
        public CSVTSVToTSpatial() {};
        public CSVTSVToTSpatial(UniformGrid uGrid, DateFormat dateFormat, String delimiter, List<Integer> csvTsvSchemaAttr)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
            this.delimiter = delimiter;
            this.csvTsvSchemaAttr = csvTsvSchemaAttr;
        };

        @Override
        public Point map(String str) throws Exception {

            // customized for ATC Shopping mall data
            //A sample tuple/record: 1351039728.980,9471001,-22366,2452,1261.421,780.711,-2.415,-2.441
            // time [ms] (unixtime + milliseconds/1000), person id, position x [mm], position y [mm], position z (height) [mm], velocity [mm/s], angle of motion [rad], facing angle [rad]

            Point spatialPoint;
            List<String> strArrayList = Arrays.asList(str.replace("\"", "").split("\\s*" + delimiter + "\\s*")); // For parsing CSV with , followed by space
            String strOId = strArrayList.get(csvTsvSchemaAttr.get(0));
            long time = Long.valueOf(strArrayList.get(csvTsvSchemaAttr.get(1)));
            double x = Double.valueOf(strArrayList.get(csvTsvSchemaAttr.get(2)));
            double y = Double.valueOf(strArrayList.get(csvTsvSchemaAttr.get(3)));;

            spatialPoint = new Point(strOId, x, y, time, uGrid);
            return spatialPoint;
        }
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

            //{"geometry": {"coordinates": [[[[-73.980455, 40.661994], [-73.980542, 40.661889], [-73.980559, 40.661897], [-73.98057, 40.661885], [-73.980611, 40.661904], [-73.9806, 40.661917], [-73.980513, 40.662022], [-73.980455, 40.661994]]]], "type": "MultiPolygon"}, "properties": {"base_bbl": "3011030028", "bin": "3026604", "cnstrct_yr": "1892", "doitt_id": "33583", "feat_code": "2100", "geomsource": "Photogramm", "groundelev": "153", "heightroof": "31.65", "lstmoddate": "2020-01-28T00:00:00.000Z", "lststatype": "Constructed", "mpluto_bbl": "3011030028", "name": null, "shape_area": "926.10935740605", "shape_len": "139.11922551796"}, "type": "Feature"}

            String json = jsonObj.get("value").toString();
            Geometry geometry;
            try {
                geometry = readGeoJSON(json);
            }
            catch (Exception e) {
                // "type" が無いStringの場合はGeometryを抽出する
                String jsonGeometry = jsonObj.get("value").get("geometry").toString();
                geometry = readGeoJSON(jsonGeometry);
            }

            Polygon spatialPolygon;
            if (geometry.getGeometryType().equalsIgnoreCase("MultiPolygon")) {
                List<List<List<Coordinate>>> listCoordinate = convertMultiCoordinates(
                        json, '[', ']', "],", ",", 4);
                spatialPolygon = new MultiPolygon(listCoordinate, uGrid);
            }
            else {
                List<List<Coordinate>> listCoordinate = convertCoordinates(
                        json, '[', ']', "],", ",", 3);
                spatialPolygon = new Polygon(listCoordinate, uGrid);
            }
            return spatialPolygon;
        }
    }

    public static class GeoJSONToTSpatialPolygon extends RichMapFunction<ObjectNode, Polygon> {

        UniformGrid uGrid;
        DateFormat dateFormat;
        String propertyTimeStamp;
        String propertyObjID;

        //ctor
        public  GeoJSONToTSpatialPolygon() {};
        public  GeoJSONToTSpatialPolygon(UniformGrid uGrid, DateFormat dateFormat, String propertyTimeStamp, String propertyObjID)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
            this.propertyTimeStamp = propertyTimeStamp;
            this.propertyObjID = propertyObjID;
        }


        @Override
        public Polygon map(ObjectNode jsonObj) throws Exception {

            String json = jsonObj.get("value").toString();
            Geometry geometry;
            try {
                geometry = readGeoJSON(jsonObj.get("value").toString());
            }
            catch (Exception e) {
                // "type" が無いStringの場合はGeometryを抽出する
                String jsonGeometry = jsonObj.get("value").get("geometry").toString();
                geometry = readGeoJSON(jsonGeometry);
            }

            JsonNode nodeProperties = jsonObj.get("value").get("properties");
            String oId = null;
            long time = 0;
            if (nodeProperties != null) {
                JsonNode nodeTime = jsonObj.get("value").get("properties").get(propertyTimeStamp);
                try {
                    if (nodeTime != null && dateFormat != null) {
                        time = dateFormat.parse(nodeTime.textValue()).getTime();
                    }
                    else if(nodeTime != null){
                        time = Long.parseLong(String.valueOf(nodeTime));
                    }
                }
                catch (ParseException e) {}

                JsonNode nodeOId = jsonObj.get("value").get("properties").get(propertyObjID);

                if (nodeOId != null) {
                    try {
                        //oId = nodeOId.textValue();
                        oId = nodeOId.toString().replaceAll("\\\"", "");
                    }
                    catch (NumberFormatException e) {}
                }
            }

            Polygon spatialPolygon;
            if (geometry.getGeometryType().equalsIgnoreCase("MultiPolygon")) {
                List<List<List<Coordinate>>> listCoodinate = convertMultiCoordinates(
                        json, '[', ']', "],", ",", 4);
                if (time != 0) {
                    //TODO: Fix timestamp to original timestamp
                    spatialPolygon = new MultiPolygon(listCoodinate, oId, time, uGrid);
                    //spatialPolygon = new MultiPolygon(listCoodinate, oId, System.currentTimeMillis(), uGrid);
                    //System.out.println("time " + time + spatialPolygon);
                }
                else {
                    spatialPolygon = new MultiPolygon(listCoodinate, oId, 0, uGrid);
                }
            }
            else {
                List<List<Coordinate>> listCoodinate = convertCoordinates(
                        json, '[', ']', "],", ",", 3);
                if (time != 0) {
                    spatialPolygon = new Polygon(oId, listCoodinate, time, uGrid);
                    //spatialPolygon = new Polygon(oId, listCoodinate, System.currentTimeMillis(), uGrid);
                    //System.out.println("print " + spatialPolygon);
                }
                else {
                    spatialPolygon = new Polygon(oId, listCoodinate, 0, uGrid);
                }
            }
            return spatialPolygon;
        }
    }

    // Assuming that csv string contains longitude and latitude at positions 0 and 1, respectively
    public static class WKTToSpatialPolygon extends RichMapFunction<String, Polygon> {

        UniformGrid uGrid;

        //ctor
        public WKTToSpatialPolygon() {};
        public WKTToSpatialPolygon(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public Polygon map(String str) throws Exception {
            // {"key":1,"value":"MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"}
            // value = "MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"

            Polygon spatialPolygon;
            int pos;
            if ((pos = str.indexOf("MULTIPOLYGON")) >= 0) {
                List<List<List<Coordinate>>> list = getMultiPolygonCoordinates(str.substring(pos));
                spatialPolygon = new MultiPolygon(list, uGrid);
            }
            else {
                pos = str.indexOf("POLYGON");
                List<List<Coordinate>> list = getPolygonCoordinates(str.substring(pos));
                spatialPolygon = new Polygon(list, uGrid);
            }
            return spatialPolygon;
        }
    }

    // Assuming that csv string contains longitude and latitude at positions 0 and 1, respectively
    public static class CSVTSVToSpatialPolygon extends RichMapFunction<String, Polygon> {

        UniformGrid uGrid;

        //ctor
        public CSVTSVToSpatialPolygon() {};
        public CSVTSVToSpatialPolygon(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public Polygon map(String str) throws Exception {
            // {"key":1,"value":"MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"}
            // value = "MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"

            Polygon spatialPolygon;
            if (str.contains("MULTIPOLYGON")) {
                List<List<List<Coordinate>>> listCoordinate = convertMultiCoordinates(
                        str, '(', ')', ",", " ", 3);
                spatialPolygon = new MultiPolygon(listCoordinate, uGrid);
            }
            else {
                List<List<Coordinate>> listCoordinate = convertCoordinates(
                        str, '(', ')', ",", " ", 2);
                spatialPolygon = new Polygon(listCoordinate, uGrid);
            }
            return spatialPolygon;
        }
    }

    public static class WKTToTSpatialPolygon extends RichMapFunction<String, Polygon> {

        UniformGrid uGrid;
        DateFormat dateFormat;
        String delimiter;

        //ctor
        public WKTToTSpatialPolygon() {};
        public WKTToTSpatialPolygon(UniformGrid uGrid, DateFormat dateFormat, String delimiter)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
            this.delimiter = delimiter;
        };

        @Override
        public Polygon map(String str) throws Exception {
            // {"key":1,"value":"MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"}
            // value = "MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"

            Polygon spatialPolygon;
            List<String> strArrayList = Arrays.asList(str.replace("\"", "").split("\\s*" + delimiter + "\\s*")); // For parsing CSV with , followed by space
            long time = 0;
            String oId = null;
            if (!strArrayList.get(0).trim().startsWith("POLYGON") && !strArrayList.get(0).trim().startsWith("MULTIPOLYGON")) {
                try {
                    oId = strArrayList.get(0).trim();
                }
                catch (NumberFormatException e) {}
            }
            if (dateFormat != null) {
                Collections.reverse(strArrayList);
                for (String s : strArrayList){
                    try {
                        time = dateFormat.parse(s.trim()).getTime();
                        break;
                    }
                    catch(ParseException e) {}
                }
            }
            int pos;
            if ((pos = str.indexOf("MULTIPOLYGON")) >= 0) {
                List<List<List<Coordinate>>> listCoordinate = getMultiPolygonCoordinates(str.substring(pos));
                if (time != 0) {
                    spatialPolygon = new MultiPolygon(listCoordinate, oId, time, uGrid);
                }
                else {
                    spatialPolygon = new MultiPolygon(listCoordinate, oId, 0, uGrid);
                }
            }
            else {
                pos = str.indexOf("POLYGON");
                List<List<Coordinate>> listCoordinate = getPolygonCoordinates(str.substring(pos));
                if (time != 0) {
                    spatialPolygon = new Polygon(oId, listCoordinate, time, uGrid);
                }
                else {
                    spatialPolygon = new Polygon(listCoordinate, uGrid);
                }
            }
            return spatialPolygon;
        }
    }

    public static DataStream<LineString> LineStringStream(DataStream inputStream, String inputType, UniformGrid uGrid){

        DataStream<LineString> lineStringStream = null;

        if(inputType.equals("GeoJSON")) {
            lineStringStream = inputStream.map(new GeoJSONToSpatialLineString(uGrid));
        }
        else if(inputType.equals("WKT")) {
            lineStringStream = inputStream.map(new WKTToSpatialLineString(uGrid)).returns(Polygon.class);
        }
        else {
            throw new IllegalArgumentException("inputType : " + inputType + " is not support");
        }

        return lineStringStream;
    }

    public static DataStream<LineString> TrajectoryStreamLineString(DataStream inputStream, String inputType, DateFormat dateFormat, String delimiter,
                                                                    String propertyTimeStamp, String propertyObjID, UniformGrid uGrid){

        DataStream<LineString> trajectoryStream = null;

        if(inputType.equals("GeoJSON")) {
            trajectoryStream = inputStream.map(new GeoJSONToTSpatialLineString(uGrid, dateFormat, propertyTimeStamp, propertyObjID));
        }
        else if(inputType.equals("WKT")) {
            trajectoryStream = inputStream.map(new WKTToTSpatialLineString(uGrid, dateFormat, delimiter)).returns(Point.class);
        }
        else {
            throw new IllegalArgumentException("inputType : " + inputType + " is not support");
        }

        return trajectoryStream;
    }

    public static class GeoJSONToSpatialLineString extends RichMapFunction<ObjectNode, LineString> {

        UniformGrid uGrid;

        //ctor
        public  GeoJSONToSpatialLineString() {};
        public  GeoJSONToSpatialLineString(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        //58> {"key":368387,"value":{"geometry":{"coordinates":[[[[-73.797919,40.681402],[-73.797885,40.681331],[-73.798032,40.681289],[-73.798048,40.681285],[-73.798067,40.681324],[-73.798075,40.681322],[-73.798092,40.681357],[-73.79806,40.681366],[-73.798058,40.681363],[-73.79801,40.681376],[-73.797919,40.681402]]]],"type":"LineString"},"properties":{"base_bbl":"4119880033","bin":"4259746","cnstrct_yr":"1955","doitt_id":"527355","feat_code":"2100","geomsource":"Photogramm","groundelev":"26","heightroof":"26.82","lstmoddate":"2017-08-22T00:00:00.000Z","lststatype":"Constructed","mpluto_bbl":"4119880033","name":null,"shape_area":"1375.27323008172","shape_len":"159.1112668769"},"type":"Feature"}}


        @Override
        public LineString map(ObjectNode jsonObj) throws Exception {
            //{"geometry": {"coordinates": [[[[-73.980455, 40.661994], [-73.980542, 40.661889], [-73.980559, 40.661897], [-73.98057, 40.661885], [-73.980611, 40.661904], [-73.9806, 40.661917], [-73.980513, 40.662022], [-73.980455, 40.661994]]]], "type": "LineString"}, "properties": {"base_bbl": "3011030028", "bin": "3026604", "cnstrct_yr": "1892", "doitt_id": "33583", "feat_code": "2100", "geomsource": "Photogramm", "groundelev": "153", "heightroof": "31.65", "lstmoddate": "2020-01-28T00:00:00.000Z", "lststatype": "Constructed", "mpluto_bbl": "3011030028", "name": null, "shape_area": "926.10935740605", "shape_len": "139.11922551796"}, "type": "Feature"}

            String json = jsonObj.get("value").toString();
            Geometry geometry;
            try {
                geometry = readGeoJSON(json);
            }
            catch (Exception e) {
                // "type" が無いStringの場合はGeometryを抽出する
                String jsonGeometry = jsonObj.get("value").get("geometry").toString();
                geometry = readGeoJSON(jsonGeometry);
            }

            LineString spatialLineString;
            if (geometry.getGeometryType().equalsIgnoreCase("MultiLineString")) {
                List<List<Coordinate>> lists = convertCoordinates(
                        json, '[', ']', "],", ",", 3);
                spatialLineString = new MultiLineString(null, lists, uGrid);
            }
            else {
                List<List<Coordinate>> parent = convertCoordinates(
                        json, '[', ']', "],", ",", 2);
                spatialLineString = new LineString(null, parent.get(0), uGrid);
            }
            return spatialLineString;
        }
    }

    public static class GeoJSONToTSpatialLineString extends RichMapFunction<ObjectNode, LineString> {

        UniformGrid uGrid;
        DateFormat dateFormat;
        String propertyTimeStamp;
        String propertyObjID;

        //ctor
        public  GeoJSONToTSpatialLineString() {};
        public  GeoJSONToTSpatialLineString(UniformGrid uGrid, DateFormat dateFormat, String propertyTimeStamp, String propertyObjID)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
            this.propertyTimeStamp = propertyTimeStamp;
            this.propertyObjID = propertyObjID;
        };

        @Override
        public LineString map(ObjectNode jsonObj) throws Exception {

            String json = jsonObj.get("value").toString();
            Geometry geometry;
            try {
                geometry = readGeoJSON(jsonObj.get("value").toString());
            }
            catch (Exception e) {
                // "type" が無いStringの場合はGeometryを抽出する
                String jsonGeometry = jsonObj.get("value").get("geometry").toString();
                geometry = readGeoJSON(jsonGeometry);
            }

            JsonNode nodeProperties = jsonObj.get("value").get("properties");
            String strOId = null;
            long time = 0;
            if (nodeProperties != null) {
                JsonNode nodeTime = jsonObj.get("value").get("properties").get(propertyTimeStamp);
                try {
                    if (nodeTime != null && dateFormat != null) {
                        time = dateFormat.parse(nodeTime.textValue()).getTime();
                    }
                    else if(nodeTime != null){
                        time = Long.parseLong(String.valueOf(nodeTime));
                    }
                }
                catch (ParseException e) {}
                JsonNode nodeOId = jsonObj.get("value").get("properties").get(propertyObjID);
                if (nodeOId != null) {
                    //strOId = nodeOId.textValue();
                    strOId = nodeOId.toString().replaceAll("\\\"", "");
                }
            }
            LineString spatialLineString;
            if (geometry.getGeometryType().equalsIgnoreCase("MultiLineString")) {
                List<List<Coordinate>> lists = convertCoordinates(
                        json, '[', ']', "],", ",", 3);
                if (time != 0) {
                    spatialLineString = new MultiLineString(strOId, lists, time, uGrid);
                }
                else {
                    spatialLineString = new MultiLineString(strOId, lists, uGrid);
                }
            }
            else {
                List<List<Coordinate>> parent = convertCoordinates(
                        json, '[', ']', "],", ",", 2);
                if (time != 0) {
                    //spatialLineString = new LineString(strOId, parent.get(0), time, uGrid);
                    //TODO: Fix timestamp to original timestamp
                    spatialLineString = new LineString(strOId, parent.get(0), time, uGrid);
                    //spatialLineString = new LineString(strOId, parent.get(0), System.currentTimeMillis(), uGrid);

                }
                else {
                    spatialLineString = new LineString(strOId, parent.get(0), uGrid);
                }
            }
            return spatialLineString;
        }
    }

    // Assuming that csv string contains longitude and latitude at positions 0 and 1, respectively
    public static class WKTToSpatialLineString extends RichMapFunction<String, LineString> {

        UniformGrid uGrid;

        //ctor
        public WKTToSpatialLineString() {};
        public WKTToSpatialLineString(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public LineString map(String str) throws Exception {
            //{"key":1,"value":"MULTILINESTRING((170.0 45.0,180.0 45.0,-180.0 45.0,-170.0, 45.0))"}

            LineString spatialLineString;
            int pos;
            if ((pos = str.indexOf("MULTILINESTRING")) >= 0) {
                List<List<Coordinate>> list = getListListCoordinates(str.substring(pos));
                spatialLineString = new MultiLineString(null, list, uGrid);
            }
            else {
                pos = str.indexOf("LINESTRING");
                List<Coordinate> list = getListCoordinates(str.substring(pos));
                spatialLineString = new LineString(null, list, uGrid);
            }
            return spatialLineString;
        }
    }

    public static class WKTToTSpatialLineString extends RichMapFunction<String, LineString> {

        UniformGrid uGrid;
        DateFormat dateFormat;
        String delimiter;

        //ctor
        public WKTToTSpatialLineString() {};
        public WKTToTSpatialLineString(UniformGrid uGrid, DateFormat dateFormat, String delimiter)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
            this.delimiter = delimiter;
        };

        @Override
        public LineString map(String str) throws Exception {

            LineString spatialLineString;
            List<String> strArrayList = Arrays.asList(str.replace("\"", "").split("\\s*" + delimiter + "\\s*")); // For parsing CSV with , followed by space
            long time = 0;
            String strOId = null;
            if (!strArrayList.get(0).trim().startsWith("LINESTRING") && !strArrayList.get(0).trim().startsWith("MULTILINESTRING")) {
                strOId = strArrayList.get(0).trim();
            }
            if (dateFormat != null) {
                Collections.reverse(strArrayList);
                for (String s : strArrayList){
                    try {
                        time = dateFormat.parse(s.trim()).getTime();
                        break;
                    }
                    catch(ParseException e) {}
                }
            }
            int pos;
            if ((pos = str.indexOf("MULTILINESTRING")) >= 0) {
                List<List<Coordinate>> list = getListListCoordinates(str.substring(pos));
                if (time != 0) {
                    spatialLineString = new MultiLineString(strOId, list, time, uGrid);
                }
                else {
                    spatialLineString = new MultiLineString(strOId, list, uGrid);
                }
            }
            else {
                pos = str.indexOf("LINESTRING");
                List<Coordinate> list = getListCoordinates(str.substring(pos));
                if (time != 0) {
                    spatialLineString = new LineString(strOId, list, time, uGrid);
                }
                else {
                    spatialLineString = new LineString(strOId, list, uGrid);
                }
            }
            return spatialLineString;
        }
    }

    public static DataStream<GeometryCollection> GeometryCollectionStream(DataStream inputStream, String inputType, UniformGrid uGrid){

        DataStream<GeometryCollection> geometryCollectionStream = null;

        if(inputType.equals("GeoJSON")) {
            geometryCollectionStream = inputStream.map(new GeoJSONToSpatialGeometryCollection(uGrid));
        }
        else if(inputType.equals("WKT")) {
            geometryCollectionStream = inputStream.map(new WKTToSpatialGeometryCollection(uGrid)).returns(Polygon.class);
        }
        else {
            throw new IllegalArgumentException("inputType : " + inputType + " is not support");
        }

        return geometryCollectionStream;
    }

    public static DataStream<GeometryCollection> TrajectoryStreamGeometryCollection(DataStream inputStream, String inputType, DateFormat dateFormat, String delimiter,
                                                                                    String propertyTimeStamp, String propertyObjID, UniformGrid uGrid){

        DataStream<GeometryCollection> trajectoryStream = null;

        if(inputType.equals("GeoJSON")) {
            trajectoryStream = inputStream.map(new GeoJSONToTSpatialGeometryCollection(uGrid, dateFormat, propertyTimeStamp, propertyObjID));
        }
        else if(inputType.equals("WKT")) {
            trajectoryStream = inputStream.map(new WKTToTSpatialGeometryCollection(uGrid, dateFormat, delimiter)).returns(Point.class);
        }
        else {
            throw new IllegalArgumentException("inputType : " + inputType + " is not support");
        }

        return trajectoryStream;
    }

    public static class GeoJSONToSpatialGeometryCollection extends RichMapFunction<ObjectNode, GeometryCollection> {

        UniformGrid uGrid;

        //ctor
        public  GeoJSONToSpatialGeometryCollection() {};
        public  GeoJSONToSpatialGeometryCollection(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public GeometryCollection map(ObjectNode jsonObj) throws Exception {
            //{"geometry": {"coordinates": [[[[-73.980455, 40.661994], [-73.980542, 40.661889], [-73.980559, 40.661897], [-73.98057, 40.661885], [-73.980611, 40.661904], [-73.9806, 40.661917], [-73.980513, 40.662022], [-73.980455, 40.661994]]]], "type": "LineString"}, "properties": {"base_bbl": "3011030028", "bin": "3026604", "cnstrct_yr": "1892", "doitt_id": "33583", "feat_code": "2100", "geomsource": "Photogramm", "groundelev": "153", "heightroof": "31.65", "lstmoddate": "2020-01-28T00:00:00.000Z", "lststatype": "Constructed", "mpluto_bbl": "3011030028", "name": null, "shape_area": "926.10935740605", "shape_len": "139.11922551796"}, "type": "Feature"}

            String json = jsonObj.get("value").toString();
            Geometry geometry;
            try {
                geometry = readGeoJSON(json);
            }
            catch (Exception e) {
                // "type" が無いStringの場合はGeometryを抽出する
                String jsonGeometry = jsonObj.get("value").get("geometry").toString();
                geometry = readGeoJSON(jsonGeometry);
            }
            List<SpatialObject> listObj = new ArrayList<SpatialObject>();
            int num = geometry.getNumGeometries();
            for (int i = 0; i < num; i++) {
                Geometry geometryN = geometry.getGeometryN(i);
                json = json.substring(json.indexOf(geometryN.getGeometryType()));
                if (geometryN.getGeometryType().equalsIgnoreCase("Point")) {
                    listObj.add(new Point(geometryN.getCoordinate().x, geometryN.getCoordinate().y, uGrid));
                }
                else if (geometryN.getGeometryType().equalsIgnoreCase("MultiPoint")) {
                    List<List<Coordinate>> parent = convertCoordinates(
                            json, '[', ']', "],", ",", 2);
                    listObj.add(new MultiPoint(null, parent.get(0), uGrid));
                }
                else if (geometryN.getGeometryType().equalsIgnoreCase("MultiPolygon")) {
                        List<List<List<Coordinate>>> listCoordinate = convertMultiCoordinates(
                                json, '[', ']', "],", ",", 4);
                        listObj.add(new MultiPolygon(listCoordinate, uGrid));
                }
                else if (geometryN.getGeometryType().equalsIgnoreCase("Polygon")) {
                    List<List<Coordinate>> listCoordinate = convertCoordinates(
                            json, '[', ']', "],", ",", 3);
                    listObj.add(new Polygon(listCoordinate, uGrid));
                }
                else if (geometryN.getGeometryType().equalsIgnoreCase("MultiLineString")) {
                    List<List<Coordinate>> listCoordinate = convertCoordinates(
                            json, '[', ']', "],", ",", 3);
                    listObj.add(new MultiLineString(null, listCoordinate, uGrid));
                }
                else if (geometryN.getGeometryType().equalsIgnoreCase("LineString")) {
                    List<List<Coordinate>> parent = convertCoordinates(
                            json, '[', ']', "],", ",", 2);
                    listObj.add(new LineString(null, parent.get(0), uGrid));
                }
            }
            GeometryCollection spatialGeometryCollection = new GeometryCollection(listObj, null);
            return spatialGeometryCollection;
        }
    }

    public static class GeoJSONToTSpatialGeometryCollection extends RichMapFunction<ObjectNode, GeometryCollection> {

        UniformGrid uGrid;
        DateFormat dateFormat;
        String propertyTimeStamp;
        String propertyObjID;

        //ctor
        public GeoJSONToTSpatialGeometryCollection() {
        }

        public GeoJSONToTSpatialGeometryCollection(UniformGrid uGrid, DateFormat dateFormat, String propertyTimeStamp, String propertyObjID) {

            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
            this.propertyTimeStamp = propertyTimeStamp;
            this.propertyObjID = propertyObjID;
        }

        @Override
        public GeometryCollection map(ObjectNode jsonObj) throws Exception {

            Geometry geometry;
            String json = jsonObj.get("value").toString();
            try {
                geometry = readGeoJSON(json);
            } catch (Exception e) {
                // "type" が無いStringの場合はGeometryを抽出する
                String jsonGeometry = jsonObj.get("value").get("geometry").toString();
                geometry = readGeoJSON(jsonGeometry);
            }
            JsonNode nodeProperties = jsonObj.get("value").get("properties");
            String strOId = null;
            long time = 0;
            if (nodeProperties != null) {
                JsonNode nodeTime = jsonObj.get("value").get("properties").get(propertyTimeStamp);
                try {
                    if (nodeTime != null && dateFormat != null) {
                        time = dateFormat.parse(nodeTime.textValue()).getTime();
                    }
                    else if(nodeTime != null){
                        time = Long.parseLong(String.valueOf(nodeTime));
                    }
                } catch (ParseException e) {
                }
                JsonNode nodeOId = jsonObj.get("value").get("properties").get(propertyObjID);
                if (nodeOId != null) {
                    //strOId = nodeOId.textValue();
                    strOId = nodeOId.toString().replaceAll("\\\"", "");
                }
            }
            List<SpatialObject> listObj = new ArrayList<SpatialObject>();
            int num = geometry.getNumGeometries();
            for (int i = 0; i < num; i++) {
                Geometry geometryN = geometry.getGeometryN(i);
                json = json.substring(1);
                json = json.substring(json.indexOf("coordinates"));
                if (geometryN.getGeometryType().equalsIgnoreCase("Point")) {
                    listObj.add(new Point(geometryN.getCoordinate().x, geometryN.getCoordinate().y, uGrid));
                }
                else if (geometryN.getGeometryType().equalsIgnoreCase("MultiPoint")) {
                    List<List<Coordinate>> parent = convertCoordinates(
                            json, '[', ']', "],", ",", 2);
                    listObj.add(new MultiPoint(null, parent.get(0), uGrid));
                }
                else if (geometryN.getGeometryType().equalsIgnoreCase("MultiPolygon")) {
                    List<List<List<Coordinate>>> listCoordinate = convertMultiCoordinates(
                            json, '[', ']', "],", ",", 4);
                    listObj.add(new MultiPolygon(listCoordinate, uGrid));
                }
                else if (geometryN.getGeometryType().equalsIgnoreCase("Polygon")) {
                    List<List<Coordinate>> listCoordinate = convertCoordinates(
                            json, '[', ']', "],", ",", 3);
                    listObj.add(new Polygon(listCoordinate, uGrid));
                }
                else if (geometryN.getGeometryType().equalsIgnoreCase("MultiLineString")) {
                    List<List<Coordinate>> listCoordinate = convertCoordinates(
                            json, '[', ']', "],", ",", 3);
                    listObj.add(new MultiLineString(null, listCoordinate, uGrid));
                }
                else if (geometryN.getGeometryType().equalsIgnoreCase("LineString")) {
                    List<List<Coordinate>> parent = convertCoordinates(
                            json, '[', ']', "],", ",", 2);
                    listObj.add(new LineString(null, parent.get(0), uGrid));
                }
            }
            GeometryCollection spatialGeometryCollection = new GeometryCollection(listObj, strOId, time);
            return spatialGeometryCollection;
        }
    }

    public static class WKTToSpatialGeometryCollection extends RichMapFunction<String, GeometryCollection> {

        UniformGrid uGrid;

        //ctor
        public WKTToSpatialGeometryCollection() {};
        public WKTToSpatialGeometryCollection(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public GeometryCollection map(String str) throws Exception {
            List<SpatialObject> listObj = new ArrayList<SpatialObject>();
            String objStr, cmpStr;
            while (0 < str.length()) {
                cmpStr = "Point";
                objStr = str.length() >= cmpStr.length() ? str.substring(0, cmpStr.length()) : str;
                if (objStr.equalsIgnoreCase(cmpStr)) {
                    Coordinate coordinate = getCoordinate(str);
                    Point point = new Point(coordinate.x,  coordinate.y, uGrid);
                    listObj.add(point);
                    str = str.substring(cmpStr.length());
                    continue;
                }
                cmpStr = "MultiPoint";
                objStr = str.length() >= cmpStr.length() ? str.substring(0, cmpStr.length()) : str;
                if (objStr.equalsIgnoreCase(cmpStr)) {
                    List<Coordinate> list = getListCoordinates(str);
                    MultiPoint multiPoint = new MultiPoint(null, list, uGrid);
                    listObj.add(multiPoint);
                    str = str.substring(cmpStr.length());
                    continue;
                }
                cmpStr = "MultiPolygon";
                objStr = str.length() >= cmpStr.length() ? str.substring(0, cmpStr.length()) : str;
                if (objStr.equalsIgnoreCase(cmpStr)) {
                    List<List<List<Coordinate>>> list = getMultiPolygonCoordinates(str);
                    MultiPolygon multiPolygon = new MultiPolygon(list, uGrid);
                    listObj.add(multiPolygon);
                    str = str.substring(cmpStr.length());
                    continue;
                }
                cmpStr = "Polygon";
                objStr = str.length() >= cmpStr.length() ? str.substring(0, cmpStr.length()) : str;
                if (objStr.equalsIgnoreCase(cmpStr)) {
                    List<List<Coordinate>> list = getPolygonCoordinates(str);
                    Polygon polygon = new Polygon(list, uGrid);
                    listObj.add(polygon);
                    str = str.substring(cmpStr.length());
                    continue;
                }
                cmpStr = "MultiLineString";
                objStr = str.length() >= cmpStr.length() ? str.substring(0, cmpStr.length()) : str;
                if (objStr.equalsIgnoreCase(cmpStr)) {
                    List<List<Coordinate>> list = getListListCoordinates(str);
                    MultiLineString multiLineString = new MultiLineString(null, list, uGrid);
                    listObj.add(multiLineString);
                    str = str.substring(cmpStr.length());
                    continue;
                }
                cmpStr = "LineString";
                objStr = str.length() >= cmpStr.length() ? str.substring(0, cmpStr.length()) : str;
                if (objStr.equalsIgnoreCase(cmpStr)) {
                    List<Coordinate> list = getListCoordinates(str);
                    LineString lineString = new LineString(null, list, uGrid);
                    listObj.add(lineString);
                    str = str.substring(cmpStr.length());
                    continue;
                }
                str = str.substring(1);
            }

            GeometryCollection spatialGeometryCollection = new GeometryCollection(listObj, null);
            return spatialGeometryCollection;
        }
    }

    public static class WKTToTSpatialGeometryCollection extends RichMapFunction<String, GeometryCollection> {

        UniformGrid uGrid;
        DateFormat dateFormat;
        String delimiter;

        //ctor
        public WKTToTSpatialGeometryCollection() {};
        public WKTToTSpatialGeometryCollection(UniformGrid uGrid, DateFormat dateFormat, String delimiter)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
            this.delimiter = delimiter;
        };

        @Override
        public GeometryCollection map(String str) throws Exception {

            List<String> strArrayList = Arrays.asList(str.replace("\"", "").split("\\s*" + delimiter + "\\s*")); // For parsing CSV with , followed by space
            long time = 0;
            String oId = null;
            if (!strArrayList.get(0).trim().startsWith("GEOMETRYCOLLECTION")) {
                try {
                    oId = strArrayList.get(0).trim();
                }
                catch (NumberFormatException e) {}
            }
            if (dateFormat != null) {
                Collections.reverse(strArrayList);
                for (String s : strArrayList){
                    try {
                        time = dateFormat.parse(s.trim()).getTime();
                        break;
                    }
                    catch(ParseException e) {}
                }
            }

            List<SpatialObject> listObj = new ArrayList<SpatialObject>();
            String objStr, cmpStr;
            while (0 < str.length()) {
                cmpStr = "Point";
                objStr = str.length() >= cmpStr.length() ? str.substring(0, cmpStr.length()) : str;
                if (objStr.equalsIgnoreCase(cmpStr)) {
                    Coordinate coordinate = getCoordinate(str);
                    Point point = new Point(oId, coordinate.x,  coordinate.y, time, uGrid);
                    listObj.add(point);
                    str = str.substring(cmpStr.length());
                    continue;
                }
                cmpStr = "MultiPoint";
                objStr = str.length() >= cmpStr.length() ? str.substring(0, cmpStr.length()) : str;
                if (objStr.equalsIgnoreCase(cmpStr)) {
                    List<Coordinate> list = getListCoordinates(str);
                    MultiPoint multiPoint = new MultiPoint(oId, list, time, uGrid);
                    listObj.add(multiPoint);
                    str = str.substring(cmpStr.length());
                    continue;
                }
                cmpStr = "MultiPolygon";
                objStr = str.length() >= cmpStr.length() ? str.substring(0, cmpStr.length()) : str;
                if (objStr.equalsIgnoreCase(cmpStr)) {
                    List<List<List<Coordinate>>> list = getMultiPolygonCoordinates(str);
                    MultiPolygon multiPolygon = new MultiPolygon(list, oId, time, uGrid);
                    listObj.add(multiPolygon);
                    str = str.substring(cmpStr.length());
                    continue;
                }
                cmpStr = "Polygon";
                objStr = str.length() >= cmpStr.length() ? str.substring(0, cmpStr.length()) : str;
                if (objStr.equalsIgnoreCase(cmpStr)) {
                    List<List<Coordinate>> list = getPolygonCoordinates(str);
                    Polygon polygon = new Polygon(oId, list, time, uGrid);
                    listObj.add(polygon);
                    str = str.substring(cmpStr.length());
                    continue;
                }
                cmpStr = "MultiLineString";
                objStr = str.length() >= cmpStr.length() ? str.substring(0, cmpStr.length()) : str;
                if (objStr.equalsIgnoreCase(cmpStr)) {
                    List<List<Coordinate>> list = getListListCoordinates(str);
                    MultiLineString multiLineString = new MultiLineString(oId, list, time, uGrid);
                    listObj.add(multiLineString);
                    str = str.substring(cmpStr.length());
                    continue;
                }
                cmpStr = "LineString";
                objStr = str.length() >= cmpStr.length() ? str.substring(0, cmpStr.length()) : str;
                if (objStr.equalsIgnoreCase(cmpStr)) {
                    List<Coordinate> list = getListCoordinates(str);
                    LineString lineString = new LineString(oId, list, time, uGrid);
                    listObj.add(lineString);
                    str = str.substring(cmpStr.length());
                    continue;
                }
                str = str.substring(1);
            }

            GeometryCollection spatialGeometryCollection = new GeometryCollection(listObj, oId, time);
            return spatialGeometryCollection;
        }
    }

    public static DataStream<MultiPoint> MultiPointStream(DataStream inputStream, String inputType, UniformGrid uGrid){

        DataStream<MultiPoint> multiPointStream = null;

        if(inputType.equals("GeoJSON")) {
            multiPointStream = inputStream.map(new GeoJSONToSpatialMultiPoint(uGrid));
        }
        else if(inputType.equals("WKT")) {
            multiPointStream = inputStream.map(new WKTToSpatialMultiPoint(uGrid)).returns(Polygon.class);
        }
        else {
            throw new IllegalArgumentException("inputType : " + inputType + " is not support");
        }

        return multiPointStream;
    }

    public static DataStream<MultiPoint> TrajectoryStreamMultiPoint(DataStream inputStream, String inputType, DateFormat dateFormat, String delimiter,
                                                                    String propertyTimeStamp, String propertyObjID, UniformGrid uGrid){

        DataStream<MultiPoint> trajectoryStream = null;

        if(inputType.equals("GeoJSON")) {
            trajectoryStream = inputStream.map(new GeoJSONToTSpatialMultiPoint(uGrid, dateFormat, propertyTimeStamp, propertyObjID));
        }
        else if(inputType.equals("WKT")) {
            trajectoryStream = inputStream.map(new WKTToTSpatialMultiPoint(uGrid, dateFormat, delimiter)).returns(Point.class);
        }
        else {
            throw new IllegalArgumentException("inputType : " + inputType + " is not support");
        }

        return trajectoryStream;
    }

    public static class GeoJSONToSpatialMultiPoint extends RichMapFunction<ObjectNode, MultiPoint> {

        UniformGrid uGrid;

        //ctor
        public  GeoJSONToSpatialMultiPoint() {};
        public  GeoJSONToSpatialMultiPoint(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public MultiPoint map(ObjectNode jsonObj) throws Exception {

            String json = jsonObj.get("value").toString();
            MultiPoint spatialMultiPoint;
                List<List<Coordinate>> parent = convertCoordinates(
                        json, '[', ']', "],", ",", 2);
            spatialMultiPoint = new MultiPoint(null, parent.get(0), uGrid);
            return spatialMultiPoint;
        }
    }

    public static class GeoJSONToTSpatialMultiPoint extends RichMapFunction<ObjectNode, MultiPoint> {

        UniformGrid uGrid;
        DateFormat dateFormat;
        String propertyTimeStamp;
        String propertyObjID;

        public  GeoJSONToTSpatialMultiPoint() {};
        public  GeoJSONToTSpatialMultiPoint(UniformGrid uGrid, DateFormat dateFormat, String propertyTimeStamp, String propertyObjID)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
            this.propertyTimeStamp = propertyTimeStamp;
            this.propertyObjID = propertyObjID;
        }

        @Override
        public MultiPoint map(ObjectNode jsonObj) throws Exception {

            String json = jsonObj.get("value").toString();
            JsonNode nodeProperties = jsonObj.get("value").get("properties");
            String strOId = null;
            long time = 0;
            if (nodeProperties != null) {
                JsonNode nodeTime = jsonObj.get("value").get("properties").get(propertyTimeStamp);
                try {
                    if (nodeTime != null && dateFormat != null) {
                        time = dateFormat.parse(nodeTime.textValue()).getTime();
                    }
                    else if(nodeTime != null){
                        time = Long.parseLong(String.valueOf(nodeTime));
                    }
                }
                catch (ParseException e) {}
                JsonNode nodeOId = jsonObj.get("value").get("properties").get(propertyObjID);
                if (nodeOId != null) {
                    //strOId = nodeOId.textValue();
                    strOId = nodeOId.toString().replaceAll("\\\"", "");
                }
            }
            MultiPoint spatialMultiPoint;
            List<List<Coordinate>> parent = convertCoordinates(
                    json, '[', ']', "],", ",", 2);
            spatialMultiPoint = new MultiPoint(strOId, parent.get(0), time, uGrid);
            return spatialMultiPoint;
        }
    }

    public static class WKTToSpatialMultiPoint extends RichMapFunction<String, MultiPoint> {

        UniformGrid uGrid;

        //ctor
        public WKTToSpatialMultiPoint() {};
        public WKTToSpatialMultiPoint(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public MultiPoint map(String str) throws Exception {

            int pos = str.indexOf("MULTIPOINT");
            List<Coordinate> list = getListCoordinates(str.substring(pos));
            MultiPoint spatialMultiPoint = new MultiPoint(null, list, uGrid);
            return spatialMultiPoint;
        }
    }

    public static class WKTToTSpatialMultiPoint extends RichMapFunction<String, MultiPoint> {

        UniformGrid uGrid;
        DateFormat dateFormat;
        String delimiter;

        //ctor
        public WKTToTSpatialMultiPoint() {};
        public WKTToTSpatialMultiPoint(UniformGrid uGrid, DateFormat dateFormat, String delimiter)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
            this.delimiter = delimiter;
        };

        @Override
        public MultiPoint map(String str) throws Exception {

            MultiPoint spatialMultiPoint;
            List<String> strArrayList = Arrays.asList(str.replace("\"", "").split("\\s*" + delimiter + "\\s*")); // For parsing CSV with , followed by space
            long time = 0;
            String strOId = null;
            if (!strArrayList.get(0).trim().startsWith("MULTIPOINT")) {
                strOId = strArrayList.get(0).trim();
            }
            if (dateFormat != null) {
                Collections.reverse(strArrayList);
                for (String s : strArrayList){
                    try {
                        time = dateFormat.parse(s.trim()).getTime();
                        break;
                    }
                    catch(ParseException e) {}
                }
            }
            int pos = str.indexOf("MULTIPOINT");
            List<Coordinate> list = getListCoordinates(str.substring(pos));
            if (time != 0) {
                spatialMultiPoint = new MultiPoint(strOId, list, time, uGrid);
            }
            else {
                spatialMultiPoint = new MultiPoint(strOId, list, uGrid);
            }
            return spatialMultiPoint;
        }
    }

    private static List<List<Coordinate>> convertCoordinates(String str, char start, char end, String separator1, String separator2, int layer) {
        // value = "MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"
        final int LAYER_NUM = layer;
        int startPos = 0;
        for (int i = 0; i < LAYER_NUM; i++) {
            startPos = str.indexOf(start, startPos);
            startPos++;
        }
        int endPos = 0;
        int count = findCountN(str, start, end);
        for (int i = 0; i < count; i++) {
            endPos = str.indexOf(end, endPos + 1);
        }
        String strCoordinates = str.substring(startPos, endPos);
        String[] arrCoordinates = strCoordinates.split(separator1);
        List<Coordinate> listChild = new ArrayList<Coordinate>();
        List<List<Coordinate>> listParent = new ArrayList<List<Coordinate>>();
        // listParent - listChild - Coordinate
        for (String s : arrCoordinates) {
            String[] arrStr = s.trim().split(separator2);
            int pos = arrStr[0].lastIndexOf(start);
            double x, y;
            if (pos < 0) {
                x = Double.parseDouble(arrStr[0]);
            }
            else {
                x = Double.parseDouble(arrStr[0].substring(pos + 1));
            }
            pos = arrStr[1].indexOf(end);
            if (pos < 0) {
                y = Double.parseDouble(arrStr[1]);
                listChild.add(new Coordinate(x, y));
            }
            else {
                y = Double.parseDouble(arrStr[1].substring(0, pos));
                listChild.add(new Coordinate(x, y));
                listParent.add(listChild);
                listChild = new ArrayList<Coordinate>();
            }
        }
        if (listChild.size() > 0) {
            listParent.add(listChild);
        }

        return listParent;
    }

    private static List<List<List<Coordinate>>> getListListListCoordinates(String str) throws Exception {
        List<List<List<Coordinate>>> listListListCoordinate = new ArrayList<>();
        WKTReader wktReader = new WKTReader();
        Geometry geometryLayer0 = wktReader.read(str);
        int num = geometryLayer0.getNumGeometries();
        for (int i = 0; i < num; i++) {
            List<List<Coordinate>> listListCoordinate = new ArrayList<>();
            Geometry geometryLayer1 = geometryLayer0.getGeometryN(i);
            int numChild = geometryLayer1.getNumGeometries();
            if (numChild > 0) {
                for (int j = 0; j < numChild; j++) {
                    Geometry geometryLayer2 = geometryLayer1.getGeometryN(i);
                    Coordinate[] coordinates = geometryLayer2.getCoordinates();
                    ArrayList<Coordinate> listCoordinate = new ArrayList<>(Arrays.asList(coordinates));
                    listListCoordinate.add(listCoordinate);
                }
            }
            else {
                Coordinate[] coordinates = geometryLayer1.getCoordinates();
                ArrayList<Coordinate> listCoordinate = new ArrayList<>(Arrays.asList(coordinates));
                listListCoordinate.add(listCoordinate);
            }
            listListListCoordinate.add(listListCoordinate);
        }
        return listListListCoordinate;
    }

    private static List<List<Coordinate>> getListListCoordinates(String str) throws Exception {
        List<List<Coordinate>> ListListCoordinate = new ArrayList<>();
        WKTReader wktReader = new WKTReader();
        Geometry geometry = wktReader.read(str);
        int num = geometry.getNumGeometries();
        for (int i = 0; i < num; i++) {
            Coordinate[] coordinates = geometry.getGeometryN(i).getCoordinates();
            ArrayList<Coordinate> listCoordinate = new ArrayList<>(Arrays.asList(coordinates));
            ListListCoordinate.add(listCoordinate);
        }
        return ListListCoordinate;
    }

    private static List<List<Coordinate>> getPolygonCoordinates(String str) throws Exception {
        List<List<Coordinate>> ListListCoordinate = new ArrayList<>();
        WKTReader wktReader = new WKTReader();
        org.locationtech.jts.geom.Polygon geomPolygon = (org.locationtech.jts.geom.Polygon)wktReader.read(str);

        org.locationtech.jts.geom.LineString externalRing = geomPolygon.getExteriorRing();
        Coordinate[] extenalCoordinates = externalRing.getCoordinates();
        ListListCoordinate.add(new ArrayList<>(Arrays.asList(extenalCoordinates)));

        int num = geomPolygon.getNumInteriorRing();
        for (int i = 0; i < num; i++) {
            org.locationtech.jts.geom.LineString internalRing = geomPolygon.getInteriorRingN(i);
            Coordinate[] internalCoordinates = internalRing.getCoordinates();
            ListListCoordinate.add(new ArrayList<>(Arrays.asList(internalCoordinates)));
        }
        return ListListCoordinate;
    }

    private static List<List<Coordinate>> getGeomPolygonCoordinates(org.locationtech.jts.geom.Polygon geomPolygon)
            throws Exception {
        List<List<Coordinate>> ListListCoordinate = new ArrayList<>();
        WKTReader wktReader = new WKTReader();

        org.locationtech.jts.geom.LineString externalRing = geomPolygon.getExteriorRing();
        Coordinate[] extenalCoordinates = externalRing.getCoordinates();
        ListListCoordinate.add(new ArrayList<>(Arrays.asList(extenalCoordinates)));

        int num = geomPolygon.getNumInteriorRing();
        for (int i = 0; i < num; i++) {
            org.locationtech.jts.geom.LineString internalRing = geomPolygon.getInteriorRingN(i);
            Coordinate[] internalCoordinates = internalRing.getCoordinates();
            ListListCoordinate.add(new ArrayList<>(Arrays.asList(internalCoordinates)));
        }
        return ListListCoordinate;
    }

    private static List<List<List<Coordinate>>> getMultiPolygonCoordinates(String str) throws Exception {
        List<List<List<Coordinate>>> listListListCoordinate = new ArrayList<>();
        WKTReader wktReader = new WKTReader();
        Geometry geometryLayer0 = wktReader.read(str);
        int num = geometryLayer0.getNumGeometries();
        for (int i = 0; i < num; i++) {
            org.locationtech.jts.geom.Polygon geomPolygon = (org.locationtech.jts.geom.Polygon)geometryLayer0.getGeometryN(i);
            List<List<Coordinate>> listListCoordinate = getGeomPolygonCoordinates(geomPolygon);
            listListListCoordinate.add(listListCoordinate);
        }
        return listListListCoordinate;
    }

    private static List<Coordinate> getListCoordinates(String str) throws Exception {
        WKTReader wktReader = new WKTReader();
        Geometry geometry = wktReader.read(str);
        Coordinate[] coordinates = geometry.getCoordinates();
        return new ArrayList<>(Arrays.asList(coordinates));
    }

    private static Coordinate getCoordinate(String str) throws Exception {
        WKTReader wktReader = new WKTReader();
        Geometry geometry = wktReader.read(str);
        return geometry.getCoordinate();
    }

    private static List<List<List<Coordinate>>> convertMultiCoordinates(String str, char start, char end, String separator1, String separator2, int layer) {
        List<String> listTarget = splitString(str, start, end);
        List<List<List<Coordinate>>> list = new ArrayList<List<List<Coordinate>>>();
        for (String target : listTarget) {
            list.add(convertCoordinates(target, start, end, separator1, separator2, layer - 1));
        }
        return list;
    }

    private static List<String> splitString(String str, char start, char end) {
        List<String> list = new ArrayList<String>();
        int startPos = str.indexOf(start);
        int endPos = 0;
        int count = findCountN(str, start, end);
        for (int i = 0; i < count; i++) {
            endPos = str.indexOf(end, endPos + 1);
        }
        String target = str.substring(startPos + 1, endPos);
        for (int i = 0, pos = 0; i < target.length(); i++) {
            for (int searchLayer = 0; i < target.length(); i++) {
                if (target.charAt(i) == start) {
                    if (searchLayer == 0) {
                        pos = i;
                    }
                    searchLayer++;
                }
                else if (target.charAt(i) == end) {
                    searchLayer--;
                    if (searchLayer <= 0) {
                        list.add(target.substring(pos, i + 1));
                        break;
                    }
                }
            }
        }
        return list;
    }

    private static Geometry readGeoJSON(String geoJson) throws org.locationtech.jts.io.ParseException {
        return geoJsonReader.read(geoJson);
    }

    private static int findCount(String target, char c) {
        return (int)target.chars().filter(ch -> ch == c).count();
    }

    private static int findCountN(String target, char start, char end) {
        int count = 0;
        target = target.substring(target.indexOf(start));
        for (int i = 0, layer = 0; i < target.length(); i++) {
            if (target.charAt(i) == start) {
                layer++;
                count++;
            }
            else if (target.charAt(i) == end) {
                layer--;
            }
            if (layer <= 0) {
                break;
            }
        }
        return count;
    }
}