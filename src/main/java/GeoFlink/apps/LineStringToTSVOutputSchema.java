package GeoFlink.apps;

import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.MultiLineString;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.locationtech.jts.geom.Coordinate;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;


public class LineStringToTSVOutputSchema implements Serializable, KafkaSerializationSchema<LineString> {

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
