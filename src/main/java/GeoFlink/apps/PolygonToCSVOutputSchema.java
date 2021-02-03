package GeoFlink.apps;

import GeoFlink.spatialObjects.MultiPolygon;
import GeoFlink.spatialObjects.Polygon;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.locationtech.jts.geom.Coordinate;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;


public class PolygonToCSVOutputSchema implements Serializable, KafkaSerializationSchema<Polygon> {

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
        if (polygon.objID != -1) {
            buf.append(polygon.objID);
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
            buf.append("((");
            Coordinate[] coordinates = polygon.polygon.getCoordinates();
            for (Coordinate c : coordinates) {
                buf.append(c.x + " " + c.y + ", ");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.deleteCharAt(buf.length() - 1);
            buf.append("))");
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
