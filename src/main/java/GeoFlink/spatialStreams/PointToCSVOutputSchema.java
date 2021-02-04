package GeoFlink.spatialStreams;

import GeoFlink.spatialObjects.Point;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;


public class PointToCSVOutputSchema implements Serializable, KafkaSerializationSchema<Point> {

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
