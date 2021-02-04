package GeoFlink.spatialStreams;

import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.MultiLineString;
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

public class LineStringToGeoJSONOutputSchema  implements Serializable, KafkaSerializationSchema<LineString> {

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