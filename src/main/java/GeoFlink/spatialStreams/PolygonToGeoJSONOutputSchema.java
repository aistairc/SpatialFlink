package GeoFlink.spatialStreams;

import GeoFlink.spatialObjects.MultiPolygon;
import GeoFlink.spatialObjects.Polygon;
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

public class PolygonToGeoJSONOutputSchema implements Serializable, KafkaSerializationSchema<Polygon> {

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
            Coordinate[] polygonCoordinates = polygon.polygon.getCoordinates();
            List<double[]> coordinates = new ArrayList<double[]>();
            for (Coordinate c : polygonCoordinates) {
                double[] coordinate = {c.x, c.y};
                coordinates.add(coordinate);
            }
            jsonGeometry.put("type", "Polygon");
            List<List<double[]>> jsonCoordinate = new ArrayList<List<double[]>>();
            jsonCoordinate.add(coordinates);
            jsonGeometry.put("coordinates", jsonCoordinate);
        }
        jsonObj.put("geometry", jsonGeometry);

        JSONObject jsonpProperties = new JSONObject();
        if (polygon.objID != -1) {
            jsonpProperties.put("oID", String.valueOf(polygon.objID));
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