package GeoFlink.spatialStreams;

import GeoFlink.spatialObjects.*;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.locationtech.jts.geom.Coordinate;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Serialization {

    public static class PointToGeoJSONOutputSchema implements Serializable, KafkaSerializationSchema<Point> {

        private String outputTopic;

        public PointToGeoJSONOutputSchema(String outputTopicName) {
            this.outputTopic = outputTopicName;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Point point, @Nullable Long timestamp) {

            JSONObject jsonObj = new JSONObject();

            JSONObject jsonGeometry = new JSONObject();
            double[] coordinate = {point.point.getX(), point.point.getY()};
            jsonGeometry.put("coordinates", coordinate);
            jsonGeometry.put("type", "Point");
            jsonObj.put("geometry", jsonGeometry);

            JSONObject jsonpProperties = new JSONObject();
            jsonpProperties.put("oID", point.objID);
            if (point.timeStampMillisec != 0) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                jsonpProperties.put("timestamp", sdf.format(new Date(point.timeStampMillisec)));
            }
            if (jsonpProperties.length() > 0) {
                jsonObj.put("properties", jsonpProperties);
            }

            jsonObj.put("type", "Feature");

            return new ProducerRecord<byte[], byte[]>(outputTopic, jsonObj.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class PointToCSVOutputSchema implements Serializable, KafkaSerializationSchema<Point> {

        private String outputTopic;

        public PointToCSVOutputSchema(String outputTopicName)
        {
            this.outputTopic = outputTopicName;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Point point, @Nullable Long timestamp) {

            final String SEPARATION = ",";
            StringBuffer buf = new StringBuffer();

            buf.append("\"");
            if (point.objID != null) {
                buf.append(point.objID);
                buf.append(SEPARATION + " ");
            }
            buf.append("POINT(");
            buf.append(point.point.getX());
            buf.append(" ");
            buf.append(point.point.getY());
            buf.append(")");
            if (point.timeStampMillisec != 0) {
                buf.append(SEPARATION + " ");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                buf.append(sdf.format(new Date(point.timeStampMillisec)));
            }
            buf.append("\"");
            buf.append(SEPARATION);

            return new ProducerRecord<byte[], byte[]>(outputTopic, buf.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class PointToTSVOutputSchema implements Serializable, KafkaSerializationSchema<Point> {

        private String outputTopic;

        public PointToTSVOutputSchema(String outputTopicName)
        {
            this.outputTopic = outputTopicName;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Point point, @Nullable Long timestamp) {

            final String SEPARATION = "\\t";
            StringBuffer buf = new StringBuffer();

            buf.append("\"");
            if (point.objID != null) {
                buf.append(point.objID);
                buf.append(SEPARATION + " ");
            }
            buf.append("POINT(");
            buf.append(point.point.getX());
            buf.append(" ");
            buf.append(point.point.getY());
            buf.append(")");
            if (point.timeStampMillisec != 0) {
                buf.append(SEPARATION + " ");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                buf.append(sdf.format(new Date(point.timeStampMillisec)));
            }
            buf.append("\"");
            buf.append(SEPARATION);

            return new ProducerRecord<byte[], byte[]>(outputTopic, buf.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class PolygonToGeoJSONOutputSchema implements Serializable, KafkaSerializationSchema<Polygon> {

        private String outputTopic;

        public PolygonToGeoJSONOutputSchema(String outputTopicName)
        {
            this.outputTopic = outputTopicName;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Polygon polygon, @Nullable Long timestamp) {

            JSONObject jsonObj = new JSONObject();

            JSONObject jsonGeometry = new JSONObject();
            if (polygon instanceof MultiPolygon) {
                List<List<Coordinate>> listCoordinate = ((MultiPolygon)polygon).getListCoordinate();
                List<List<List<double[]>>> jsonCoordinate = new ArrayList<List<List<double[]>>>();
                for (List<Coordinate> l : listCoordinate) {
                    List<double[]> arrCoordinate = new ArrayList<double[]>();
                    for (Coordinate c : l) {
                        double[] coordinate = {c.x, c.y};
                        arrCoordinate.add(coordinate);
                    }
                    List<List<double[]>> coordinates = new ArrayList<List<double[]>>();
                    coordinates.add(arrCoordinate);
                    jsonCoordinate.add(coordinates);
                }
                jsonGeometry.put("type", "MultiPolygon");
                jsonGeometry.put("coordinates", jsonCoordinate);
            }
            else {
                List<List<double[]>> jsonCoordinate = new ArrayList<List<double[]>>();
                for (org.locationtech.jts.geom.Polygon p : polygon.polygon) {
                    Coordinate[] polygonCoordinates = p.getCoordinates();
                    List<double[]> coordinates = new ArrayList<double[]>();
                    for (Coordinate c : polygonCoordinates) {
                        double[] coordinate = {c.x, c.y};
                        coordinates.add(coordinate);
                    }
                    jsonCoordinate.add(coordinates);
                }
                jsonGeometry.put("type", "Polygon");
                jsonGeometry.put("coordinates", jsonCoordinate);
            }
            jsonObj.put("geometry", jsonGeometry);

            JSONObject jsonpProperties = new JSONObject();
            if (polygon.lObjID != -1) {
                jsonpProperties.put("oID", String.valueOf(polygon.lObjID));
            }
            if (polygon.timeStampMillisec != 0) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                jsonpProperties.put("timestamp", sdf.format(new Date(polygon.timeStampMillisec)));
            }
            if (jsonpProperties.length() > 0) {
                jsonObj.put("properties", jsonpProperties);
            }

            jsonObj.put("type", "Feature");

            return new ProducerRecord<byte[], byte[]>(outputTopic, jsonObj.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class PolygonToCSVOutputSchema implements Serializable, KafkaSerializationSchema<Polygon> {

        private String outputTopic;

        public PolygonToCSVOutputSchema(String outputTopicName)
        {
            this.outputTopic = outputTopicName;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Polygon polygon, @Nullable Long timestamp) {

            final String SEPARATION = ",";
            StringBuffer buf = new StringBuffer();

            buf.append("\"");
            if (polygon.lObjID != -1) {
                buf.append(polygon.lObjID);
                buf.append(SEPARATION + " ");
            }
            if (polygon instanceof MultiPolygon) {
                buf.append("MULTIPOLYGON");
                buf.append("(");
                List<List<Coordinate>> listCoordinate = ((MultiPolygon)polygon).getListCoordinate();
                for (List<Coordinate> l : listCoordinate) {
                    buf.append("((");
                    for (Coordinate c : l) {
                        buf.append(c.x + " " + c.y + ", ");
                    }
                    buf.deleteCharAt(buf.length() - 1);
                    buf.deleteCharAt(buf.length() - 1);
                    buf.append(")),");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.append(")");
            }
            else {
                buf.append("POLYGON");
                buf.append("(");
                for (org.locationtech.jts.geom.Polygon p : polygon.polygon) {
                    buf.append("(");
                    Coordinate[] coordinates = p.getCoordinates();
                    for (Coordinate c : coordinates) {
                        buf.append(c.x + " " + c.y + ", ");
                    }
                    buf.deleteCharAt(buf.length() - 1);
                    buf.deleteCharAt(buf.length() - 1);
                    buf.append("), ");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.deleteCharAt(buf.length() - 1);
                buf.append(")");
            }
            if (polygon.timeStampMillisec != 0) {
                buf.append(SEPARATION + " ");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                buf.append(sdf.format(new Date(polygon.timeStampMillisec)));
            }
            buf.append("\"");
            buf.append(SEPARATION);

            return new ProducerRecord<byte[], byte[]>(outputTopic, buf.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class PolygonToTSVOutputSchema implements Serializable, KafkaSerializationSchema<Polygon> {

        private String outputTopic;

        public PolygonToTSVOutputSchema(String outputTopicName)
        {
            this.outputTopic = outputTopicName;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(Polygon polygon, @Nullable Long timestamp) {

            final String SEPARATION = "\\t";
            StringBuffer buf = new StringBuffer();

            buf.append("\"");
            if (polygon.lObjID != -1) {
                buf.append(polygon.lObjID);
                buf.append(SEPARATION + " ");
            }
            if (polygon instanceof MultiPolygon) {
                buf.append("MULTIPOLYGON");
                buf.append("(");
                List<List<Coordinate>> listCoordinate = ((MultiPolygon)polygon).getListCoordinate();
                for (List<Coordinate> l : listCoordinate) {
                    buf.append("((");
                    for (Coordinate c : l) {
                        buf.append(c.x + " " + c.y + ", ");
                    }
                    buf.deleteCharAt(buf.length() - 1);
                    buf.deleteCharAt(buf.length() - 1);
                    buf.append(")),");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.append(")");
            }
            else {
                buf.append("POLYGON");
                buf.append("(");
                for (org.locationtech.jts.geom.Polygon p : polygon.polygon) {
                    buf.append("(");
                    Coordinate[] coordinates = p.getCoordinates();
                    for (Coordinate c : coordinates) {
                        buf.append(c.x + " " + c.y + ", ");
                    }
                    buf.deleteCharAt(buf.length() - 1);
                    buf.deleteCharAt(buf.length() - 1);
                    buf.append("), ");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.deleteCharAt(buf.length() - 1);
                buf.append(")");
            }
            if (polygon.timeStampMillisec != 0) {
                buf.append(SEPARATION + " ");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                buf.append(sdf.format(new Date(polygon.timeStampMillisec)));
            }
            buf.append("\"");
            buf.append(SEPARATION);

            return new ProducerRecord<byte[], byte[]>(outputTopic, buf.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class LineStringToGeoJSONOutputSchema  implements Serializable, KafkaSerializationSchema<LineString> {

        private String outputTopic;

        public LineStringToGeoJSONOutputSchema(String outputTopicName)
        {
            this.outputTopic = outputTopicName;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(LineString lineString, @Nullable Long timestamp) {

            JSONObject jsonObj = new JSONObject();

            JSONObject jsonGeometry = new JSONObject();
            if (lineString instanceof MultiLineString) {
                List<List<Coordinate>> listCoordinate = ((MultiLineString)lineString).getListCoordinate();
                List<List<double[]>> jsonCoordinate = new ArrayList<List<double[]>>();
                for (List<Coordinate> l : listCoordinate) {
                    List<double[]> arrCoordinate = new ArrayList<double[]>();
                    for (Coordinate c : l) {
                        double[] coordinate = {c.x, c.y};
                        arrCoordinate.add(coordinate);
                    }
                    jsonCoordinate.add(arrCoordinate);
                }
                jsonGeometry.put("type", "MultiLineString");
                jsonGeometry.put("coordinates", jsonCoordinate);
            }
            else {
                Coordinate[] lineStringCoordinates = lineString.lineString.getCoordinates();
                List<double[]> jsonCoordinate = new ArrayList<double[]>();
                for (Coordinate c : lineStringCoordinates) {
                    double[] coordinate = {c.x, c.y};
                    jsonCoordinate.add(coordinate);
                }
                jsonGeometry.put("type", "LineString");
                jsonGeometry.put("coordinates", jsonCoordinate);
            }
            jsonObj.put("geometry", jsonGeometry);

            JSONObject jsonpProperties = new JSONObject();
            if (lineString.objID != null) {
                jsonpProperties.put("oID", lineString.objID);
            }
            if (lineString.timeStampMillisec != 0) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                jsonpProperties.put("timestamp", sdf.format(new Date(lineString.timeStampMillisec)));
            }
            if (jsonpProperties.length() > 0) {
                jsonObj.put("properties", jsonpProperties);
            }

            jsonObj.put("type", "Feature");

            return new ProducerRecord<byte[], byte[]>(outputTopic, jsonObj.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class LineStringToCSVOutputSchema implements Serializable, KafkaSerializationSchema<LineString> {

        private String outputTopic;

        public LineStringToCSVOutputSchema(String outputTopicName)
        {
            this.outputTopic = outputTopicName;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(LineString lineString, @Nullable Long timestamp) {

            final String SEPARATION = ",";
            StringBuffer buf = new StringBuffer();

            buf.append("\"");
            if (lineString.objID != null) {
                buf.append(lineString.objID);
                buf.append(SEPARATION + " ");
            }
            if (lineString instanceof MultiLineString) {
                buf.append("MULTILINESTRING");
                buf.append("(");
                List<List<Coordinate>> listCoordinate = ((MultiLineString)lineString).getListCoordinate();
                for (List<Coordinate> l : listCoordinate) {
                    buf.append("(");
                    for (Coordinate c : l) {
                        buf.append(c.x + " " + c.y + ", ");
                    }
                    buf.deleteCharAt(buf.length() - 1);
                    buf.deleteCharAt(buf.length() - 1);
                    buf.append("),");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.append(")");
            }
            else {
                buf.append("LINESTRING");
                buf.append("(");
                Coordinate[] coordinates = lineString.lineString.getCoordinates();
                for (Coordinate c : coordinates) {
                    buf.append(c.x + " " + c.y + ", ");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.deleteCharAt(buf.length() - 1);
                buf.append(")");
            }
            if (lineString.timeStampMillisec != 0) {
                buf.append(SEPARATION + " ");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                buf.append(sdf.format(new Date(lineString.timeStampMillisec)));
            }
            buf.append("\"");
            buf.append(SEPARATION);

            return new ProducerRecord<byte[], byte[]>(outputTopic, buf.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class LineStringToTSVOutputSchema implements Serializable, KafkaSerializationSchema<LineString> {

        private String outputTopic;

        public LineStringToTSVOutputSchema(String outputTopicName)
        {
            this.outputTopic = outputTopicName;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(LineString lineString, @Nullable Long timestamp) {

            final String SEPARATION = "\\t";
            StringBuffer buf = new StringBuffer();

            buf.append("\"");
            if (lineString.objID != null) {
                buf.append(lineString.objID);
                buf.append(SEPARATION + " ");
            }
            if (lineString instanceof MultiLineString) {
                buf.append("MULTILINESTRING");
                buf.append("(");
                List<List<Coordinate>> listCoordinate = ((MultiLineString)lineString).getListCoordinate();
                for (List<Coordinate> l : listCoordinate) {
                    buf.append("(");
                    for (Coordinate c : l) {
                        buf.append(c.x + " " + c.y + ", ");
                    }
                    buf.deleteCharAt(buf.length() - 1);
                    buf.deleteCharAt(buf.length() - 1);
                    buf.append("),");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.append(")");
            }
            else {
                buf.append("LINESTRING");
                buf.append("(");
                Coordinate[] coordinates = lineString.lineString.getCoordinates();
                for (Coordinate c : coordinates) {
                    buf.append(c.x + " " + c.y + ", ");
                }
                buf.deleteCharAt(buf.length() - 1);
                buf.deleteCharAt(buf.length() - 1);
                buf.append(")");
            }
            if (lineString.timeStampMillisec != 0) {
                buf.append(SEPARATION + " ");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                buf.append(sdf.format(new Date(lineString.timeStampMillisec)));
            }
            buf.append("\"");
            buf.append(SEPARATION);

            return new ProducerRecord<byte[], byte[]>(outputTopic, buf.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
}
