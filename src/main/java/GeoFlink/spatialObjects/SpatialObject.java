package GeoFlink.spatialObjects;

import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;

public class SpatialObject extends Object implements Serializable {

    public int objID;
    public String timeStamp;

    public SpatialObject() {}

    static class getoID implements MapFunction<Point, Integer> {
        @Override
        public Integer map(Point p) throws Exception {
            return p.objID;
        }
    }
}

