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
            pointStream = inputStream.map(new GeoJSONToSpatial(uGrid));
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

            int objID = json.get("value").get("properties").get("oID").asInt();
            String objType = json.get("value").get("geometry").get("type").asText();
            String timeStamp = json.get("value").get("properties").get("timestamp").asText();
            Point myPoint = new Point(objID, json.get("value").get("geometry").get("coordinates").get(0).asDouble(), json.get("value").get("geometry").get("coordinates").get(1).asDouble(), timeStamp, uGrid);

            return myPoint;
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

            int objID = Integer.parseInt(strArrayList.get(0));
            String timeStamp = strArrayList.get(1);
            Point myPoint = new Point(objID, Double.parseDouble(strArrayList.get(2)), Double.parseDouble(strArrayList.get(3)), timeStamp, uGrid);

            return myPoint;
        }
    }

}


