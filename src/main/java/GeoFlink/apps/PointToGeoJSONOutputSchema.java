package GeoFlink.apps;

import GeoFlink.spatialObjects.Point;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

import org.json.*;


public class PointToGeoJSONOutputSchema implements Serializable, KafkaSerializationSchema<Point> {

    private String outputTopic;

    public PointToGeoJSONOutputSchema(String outputTopicName)
    {
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
