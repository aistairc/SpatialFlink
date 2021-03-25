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

    public static DataStream<Point> PointStream(DataStream inputStream, String inputType, UniformGrid uGrid){

        DataStream<Point> pointStream = null;

        if(inputType.equals("GeoJSON")) {
            pointStream = inputStream.map(new GeoJSONToSpatial(uGrid));
        }
        else if (inputType.equals("CSV")){
            pointStream = inputStream.map(new CSVToSpatial(uGrid));
        }
        else if (inputType.equals("TSV")){
            pointStream = inputStream.map(new TSVToSpatial(uGrid));
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
        else if (inputType.equals("TSV")){
            trajectoryStream = inputStream.map(new TSVToTSpatial(uGrid, dateFormat));
        }

        return trajectoryStream;
    }

    public static DataStream<Polygon> PolygonStream(DataStream inputStream, String inputType, UniformGrid uGrid){

        DataStream<Polygon> polygonStream = null;

        if(inputType.equals("GeoJSON")) {
            polygonStream = inputStream.map(new GeoJSONToSpatialPolygon(uGrid));
        }
        else if (inputType.equals("CSV")){
            polygonStream = inputStream.map(new CSVToSpatialPolygon(uGrid));
        }
        else if (inputType.equals("TSV")){
            polygonStream = inputStream.map(new TSVToSpatialPolygon(uGrid));
        }

        return polygonStream;
    }

    public static DataStream<Polygon> TrajectoryStreamPolygon(DataStream inputStream, String inputType, DateFormat dateFormat, UniformGrid uGrid){

        DataStream<Polygon> trajectoryStream = null;

        if(inputType.equals("GeoJSON")) {
            trajectoryStream = inputStream.map(new GeoJSONToTSpatialPolygon(uGrid, dateFormat));
        }
        else if (inputType.equals("CSV")){
            trajectoryStream = inputStream.map(new CSVToTSpatialPolygon(uGrid, dateFormat));
        }
        else if (inputType.equals("TSV")){
            trajectoryStream = inputStream.map(new TSVToTSpatialPolygon(uGrid, dateFormat));
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

        //ctor
        public  GeoJSONToTSpatial() {};
        public  GeoJSONToTSpatial(UniformGrid uGrid, DateFormat dateFormat)
        {

            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
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
                JsonNode nodeTime = jsonObj.get("value").get("properties").get("timestamp");
                try {
                    if (nodeTime != null && dateFormat != null) {
                        time = dateFormat.parse(nodeTime.textValue()).getTime();
                    }
                }
                catch (ParseException e) {}
                JsonNode nodeOId = jsonObj.get("value").get("properties").get("oID");
                if (nodeOId != null) {
                    strOId = nodeOId.textValue();
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
            Coordinate coordinate = getCoordinateFromPoint(strTuple.toString());
            Point spatialPoint = new Point(coordinate.x,  coordinate.y, uGrid);
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
            Coordinate coordinate = getCoordinateFromPoint(strTuple.toString());
            List<String> strArrayList = Arrays.asList(strTuple.get("value").toString().replace("\"", "").split("\\s*,\\s*")); // For parsing CSV with , followed by space
            long time = 0;
            String strOId = null;
            if (!strArrayList.get(0).startsWith("POINT")) {
                strOId = strArrayList.get(0).trim();
            }
            if (dateFormat != null) {
                Collections.reverse(strArrayList);
                for (String str : strArrayList){
                    try {
                        time = dateFormat.parse(str.trim()).getTime();
                        break;
                    }
                    catch(ParseException e) {}
                }
            }
            if (time != 0) {
                spatialPoint = new Point(strOId, coordinate.x, coordinate.y, time, uGrid);
            }
            else {
                spatialPoint = new Point(strOId, coordinate.x, coordinate.y, 0, uGrid);
            }
            return spatialPoint;
        }
    }

    // Assuming that tsv string contains longitude and latitude at positions 0 and 1, respectively
    public static class TSVToSpatial extends RichMapFunction<ObjectNode, Point> {

        UniformGrid uGrid;

        //ctor
        public  TSVToSpatial() {};
        public  TSVToSpatial(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public Point map(ObjectNode strTuple) throws Exception {
            Coordinate coordinate = getCoordinateFromPoint(strTuple.toString());
            Point spatialPoint = new Point(coordinate.x,  coordinate.y, uGrid);
            return spatialPoint;
        }
    }

    // Assuming that tsv string contains longitude and latitude at positions 0 and 1, respectively
    public static class TSVToTSpatial extends RichMapFunction<ObjectNode, Point> {

        UniformGrid uGrid;
        DateFormat dateFormat;

        //ctor
        public  TSVToTSpatial() {};
        public  TSVToTSpatial(UniformGrid uGrid, DateFormat dateFormat)
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
            Coordinate coordinate = getCoordinateFromPoint(strTuple.toString());
            List<String> strArrayList = Arrays.asList(strTuple.get("value").toString().replace("\"", "").split("\\\\t")); // For parsing TSV with \t
            long time = 0;
            String strOId = null;
            if (!strArrayList.get(0).trim().startsWith("POINT")) {
                strOId = strArrayList.get(0).trim();
            }
            if (dateFormat != null) {
                Collections.reverse(strArrayList);
                for (String str : strArrayList){
                    try {
                        time = dateFormat.parse(str.trim()).getTime();
                        break;
                    }
                    catch(ParseException e) {}
                }
            }
            if (time != 0) {
                spatialPoint = new Point(strOId, coordinate.x, coordinate.y, time, uGrid);
            }
            else {
                spatialPoint = new Point(strOId, coordinate.x, coordinate.y, 0, uGrid);
            }
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

        //ctor
        public  GeoJSONToTSpatialPolygon() {};
        public  GeoJSONToTSpatialPolygon(UniformGrid uGrid, DateFormat dateFormat)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
        };

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
                JsonNode nodeTime = jsonObj.get("value").get("properties").get("lstmoddate");
                try {
                    if (nodeTime != null && dateFormat != null) {
                        time = dateFormat.parse(nodeTime.textValue()).getTime();
                    }
                }
                catch (ParseException e) {}
                JsonNode nodeOId = jsonObj.get("value").get("properties").get("oID");
                if (nodeOId != null) {
                    try {
                        oId = nodeOId.textValue();
                    }
                    catch (NumberFormatException e) {}
                }
            }

            Polygon spatialPolygon;
            if (geometry.getGeometryType().equalsIgnoreCase("MultiPolygon")) {
                List<List<List<Coordinate>>> listCoodinate = convertMultiCoordinates(
                        json, '[', ']', "],", ",", 4);
                if (time != 0) {
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
            // {"key":1,"value":"MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"}
            // value = "MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"

            Polygon spatialPolygon;
            if (strTuple.get("value").toString().contains("MULTIPOLYGON")) {
                List<List<List<Coordinate>>> listCoordinate = convertMultiCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 3);
                spatialPolygon = new MultiPolygon(listCoordinate, uGrid);
            }
            else {
                List<List<Coordinate>> listCoordinate = convertCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 2);
                spatialPolygon = new Polygon(listCoordinate, uGrid);
            }
            return spatialPolygon;
        }
    }

    public static class CSVToTSpatialPolygon extends RichMapFunction<ObjectNode, Polygon> {

        UniformGrid uGrid;
        DateFormat dateFormat;

        //ctor
        public  CSVToTSpatialPolygon() {};
        public  CSVToTSpatialPolygon(UniformGrid uGrid, DateFormat dateFormat)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
        };

        @Override
        public Polygon map(ObjectNode strTuple) throws Exception {
            // {"key":1,"value":"MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"}
            // value = "MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"

            Polygon spatialPolygon;
            List<String> strArrayList = Arrays.asList(strTuple.get("value").toString().replace("\"", "").split("\\s*,\\s*")); // For parsing CSV with , followed by space
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
                for (String str : strArrayList){
                    try {
                        time = dateFormat.parse(str.trim()).getTime();
                        break;
                    }
                    catch(ParseException e) {}
                }
            }
            if (strTuple.get("value").toString().contains("MULTIPOLYGON")) {
                List<List<List<Coordinate>>> listCoordinate = convertMultiCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 3);
                if (time != 0) {
                    spatialPolygon = new MultiPolygon(listCoordinate, oId, time, uGrid);
                }
                else {
                    spatialPolygon = new MultiPolygon(listCoordinate, oId, 0, uGrid);
                }
            }
            else {
                List<List<Coordinate>> listCoordinate = convertCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 2);
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

    // Assuming that tsv string contains longitude and latitude at positions 0 and 1, respectively
    public static class TSVToSpatialPolygon extends RichMapFunction<ObjectNode, Polygon> {

        UniformGrid uGrid;

        //ctor
        public  TSVToSpatialPolygon() {};
        public  TSVToSpatialPolygon(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public Polygon map(ObjectNode strTuple) throws Exception {
            // {"key":1,"value":"MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"}
            // value = "MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"

            Polygon spatialPolygon;
            if (strTuple.get("value").toString().contains("MULTIPOLYGON")) {
                List<List<List<Coordinate>>> listCoordinate = convertMultiCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 3);
                spatialPolygon = new MultiPolygon(listCoordinate, uGrid);
            }
            else {
                List<List<Coordinate>> listCoordinate = convertCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 2);
                spatialPolygon = new Polygon(listCoordinate, uGrid);
            }
            return spatialPolygon;
        }
    }

    public static class TSVToTSpatialPolygon extends RichMapFunction<ObjectNode, Polygon> {

        UniformGrid uGrid;
        DateFormat dateFormat;

        //ctor
        public  TSVToTSpatialPolygon() {};
        public  TSVToTSpatialPolygon(UniformGrid uGrid, DateFormat dateFormat)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
        };

        @Override
        public Polygon map(ObjectNode strTuple) throws Exception {
            // {"key":1,"value":"MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"}
            // value = "MULTIPOLYGON (((-74.15010482037168 40.62183511874645, -74.15016701565006 40.62177739783489, -74.1502116609276 40.62180593197037, -74.15015270982748 40.62185788893257, -74.15014748371995 40.62186259918266, -74.1501238625006 40.6218473986088, -74.150107093191 40.62186251414858, -74.15008804280825 40.621850243299434, -74.15010482037168 40.62183511874645)))"

            Polygon spatialPolygon;
            List<String> strArrayList = Arrays.asList(strTuple.get("value").toString().replace("\"", "").split("\\\\t")); // For parsing TSV with \t
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
                for (String str : strArrayList){
                    try {
                        time = dateFormat.parse(str.trim()).getTime();
                        break;
                    }
                    catch(ParseException e) {}
                }
            }
            if (strTuple.get("value").toString().contains("MULTIPOLYGON")) {
                List<List<List<Coordinate>>> listCoordinate = convertMultiCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 3);
                if (time != 0) {
                    spatialPolygon = new MultiPolygon(listCoordinate, oId, time, uGrid);
                }
                else {
                    spatialPolygon = new MultiPolygon(listCoordinate, oId, 0, uGrid);
                }
            }
            else {
                List<List<Coordinate>> listCoordinate = convertCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 2);
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
        else if (inputType.equals("CSV")){
            lineStringStream = inputStream.map(new CSVToSpatialLineString(uGrid));
        }
        else if (inputType.equals("TSV")){
            lineStringStream = inputStream.map(new TSVToSpatialLineString(uGrid));
        }

        return lineStringStream;
    }

    public static DataStream<LineString> TrajectoryStreamLineString(DataStream inputStream, String inputType, DateFormat dateFormat, UniformGrid uGrid){

        DataStream<LineString> trajectoryStream = null;

        if(inputType.equals("GeoJSON")) {
            trajectoryStream = inputStream.map(new GeoJSONToTSpatialLineString(uGrid, dateFormat));
        }
        else if (inputType.equals("CSV")){
            trajectoryStream = inputStream.map(new CSVToTSpatialLineString(uGrid, dateFormat));
        }
        else if (inputType.equals("TSV")){
            trajectoryStream = inputStream.map(new TSVToTSpatialLineString(uGrid, dateFormat));
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

        //ctor
        public  GeoJSONToTSpatialLineString() {};
        public  GeoJSONToTSpatialLineString(UniformGrid uGrid, DateFormat dateFormat)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
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
                JsonNode nodeTime = jsonObj.get("value").get("properties").get("timestamp");
                try {
                    if (nodeTime != null && dateFormat != null) {
                        time = dateFormat.parse(nodeTime.textValue()).getTime();
                    }
                }
                catch (ParseException e) {}
                JsonNode nodeOId = jsonObj.get("value").get("properties").get("oID");
                if (nodeOId != null) {
                    strOId = nodeOId.textValue();
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
    public static class CSVToSpatialLineString extends RichMapFunction<ObjectNode, LineString> {

        UniformGrid uGrid;

        //ctor
        public  CSVToSpatialLineString() {};
        public  CSVToSpatialLineString(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public LineString map(ObjectNode strTuple) throws Exception {
            //{"key":1,"value":"MULTILINESTRING((170.0 45.0,180.0 45.0,-180.0 45.0,-170.0, 45.0))"}

            LineString spatialLineString;
            if (strTuple.get("value").toString().contains("MULTILINESTRING")) {
                List<List<Coordinate>> list = convertCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 2);
                spatialLineString = new MultiLineString(null, list, uGrid);
            }
            else {
                List<List<Coordinate>> parent = convertCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 1);
                spatialLineString = new LineString(null, parent.get(0), uGrid);
            }
            return spatialLineString;
        }
    }

    public static class CSVToTSpatialLineString extends RichMapFunction<ObjectNode, LineString> {

        UniformGrid uGrid;
        DateFormat dateFormat;

        //ctor
        public  CSVToTSpatialLineString() {};
        public  CSVToTSpatialLineString(UniformGrid uGrid, DateFormat dateFormat)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
        };

        @Override
        public LineString map(ObjectNode strTuple) throws Exception {

            LineString spatialLineString;
            List<String> strArrayList = Arrays.asList(strTuple.get("value").toString().replace("\"", "").split("\\s*,\\s*")); // For parsing CSV with , followed by space
            long time = 0;
            String strOId = null;
            if (!strArrayList.get(0).trim().startsWith("LINESTRING") && !strArrayList.get(0).trim().startsWith("MULTILINESTRING")) {
                strOId = strArrayList.get(0).trim();
            }
            if (dateFormat != null) {
                Collections.reverse(strArrayList);
                for (String str : strArrayList){
                    try {
                        time = dateFormat.parse(str.trim()).getTime();
                        break;
                    }
                    catch(ParseException e) {}
                }
            }
            if (strTuple.get("value").toString().contains("MULTILINESTRING")) {
                List<List<Coordinate>> lists = convertCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 2);
                if (time != 0) {
                    spatialLineString = new MultiLineString(strOId, lists, time, uGrid);
                }
                else {
                    spatialLineString = new MultiLineString(strOId, lists, uGrid);
                }
            }
            else {
                List<List<Coordinate>> lists = convertCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 1);
                if (time != 0) {
                    spatialLineString = new LineString(strOId, lists.get(0), time, uGrid);
                }
                else {
                    spatialLineString = new LineString(strOId, lists.get(0), uGrid);
                }
            }
            return spatialLineString;
        }
    }

    // Assuming that tsv string contains longitude and latitude at positions 0 and 1, respectively
    public static class TSVToSpatialLineString extends RichMapFunction<ObjectNode, LineString> {

        UniformGrid uGrid;

        //ctor
        public  TSVToSpatialLineString() {};
        public  TSVToSpatialLineString(UniformGrid uGrid)
        {
            this.uGrid = uGrid;
        };

        @Override
        public LineString map(ObjectNode strTuple) throws Exception {
            //{"key":1,"value":"MULTILINESTRING((170.0 45.0,180.0 45.0,-180.0 45.0,-170.0, 45.0))"}

            LineString spatialLineString;
            if (strTuple.get("value").toString().contains("MULTILINESTRING")) {
                List<List<Coordinate>> list = convertCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 2);
                spatialLineString = new MultiLineString(null, list, uGrid);
            }
            else {
                List<List<Coordinate>> parent = convertCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 1);
                spatialLineString = new LineString(null, parent.get(0), uGrid);
            }
            return spatialLineString;
        }
    }

    public static class TSVToTSpatialLineString extends RichMapFunction<ObjectNode, LineString> {

        UniformGrid uGrid;
        DateFormat dateFormat;

        //ctor
        public  TSVToTSpatialLineString() {};
        public  TSVToTSpatialLineString(UniformGrid uGrid, DateFormat dateFormat)
        {
            this.uGrid = uGrid;
            this.dateFormat = dateFormat;
        };

        @Override
        public LineString map(ObjectNode strTuple) throws Exception {

            LineString spatialLineString;
            List<String> strArrayList = Arrays.asList(strTuple.get("value").toString().replace("\"", "").split("\\\\t")); // For parsing TSV with \t followed by space
            long time = 0;
            String strOId = null;
            if (!strArrayList.get(0).trim().startsWith("LINESTRING") && !strArrayList.get(0).trim().startsWith("MULTILINESTRING")) {
                strOId = strArrayList.get(0).trim();
            }
            if (dateFormat != null) {
                Collections.reverse(strArrayList);
                for (String str : strArrayList){
                    try {
                        time = dateFormat.parse(str.trim()).getTime();
                        break;
                    }
                    catch(ParseException e) {}
                }
            }
            if (strTuple.get("value").toString().contains("MULTILINESTRING")) {
                List<List<Coordinate>> lists = convertCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 2);
                if (time != 0) {
                    spatialLineString = new MultiLineString(strOId, lists, time, uGrid);
                }
                else {
                    spatialLineString = new MultiLineString(strOId, lists, uGrid);
                }
            }
            else {
                List<List<Coordinate>> lists = convertCoordinates(
                        strTuple.get("value").toString(), '(', ')', ",", " ", 1);
                if (time != 0) {
                    spatialLineString = new LineString(strOId, lists.get(0), time, uGrid);
                }
                else {
                    spatialLineString = new LineString(strOId, lists.get(0), uGrid);
                }
            }
            return spatialLineString;
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
        int count = findCount(str, start);
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
        int count = findCount(str, start);
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
                    continue;
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

    private static Coordinate getCoordinateFromPoint(String str) {
        // "POINT(116.69171 39.85184)"
        final int LAYER_NUM = 1;
        int startPos = 0;
        for (int i = 0; i < LAYER_NUM; i++) {
            startPos = str.indexOf("(", startPos);
            startPos++;
        }
        int endPos = 0;
        for (int i = 0; i < LAYER_NUM; i++) {
            endPos = str.indexOf(")", endPos + 1);
        }
        String strCoordinates = str.substring(startPos, endPos);
        String[] arrStr = strCoordinates.trim().split("\\s* \\s*");
        int pos = arrStr[0].lastIndexOf("(");
        double x, y;
        if (pos < 0) {
            x = Double.parseDouble(arrStr[0]);
        }
        else {
            x = Double.parseDouble(arrStr[0].substring(pos + 1));
        }
        pos = arrStr[1].indexOf(")");
        if (pos < 0) {
            y = Double.parseDouble(arrStr[1]);
        }
        else {
            y = Double.parseDouble(arrStr[1].substring(0, pos));
        }
        return new Coordinate(x, y);
    }

    private static Geometry readGeoJSON(String geoJson) throws org.locationtech.jts.io.ParseException {
        return geoJsonReader.read(geoJson);
    }

    private static int findCount(String target, char c) {
        return (int)target.chars().filter(ch -> ch == c).count();
    }
}