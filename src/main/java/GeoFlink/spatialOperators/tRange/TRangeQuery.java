package GeoFlink.spatialOperators.tRange;

import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.SpatialOperator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public abstract class TRangeQuery<T extends SpatialObject, K extends SpatialObject> extends SpatialOperator implements Serializable {
    private QueryConfiguration queryConfiguration;
    public QueryConfiguration getQueryConfiguration() {
        return queryConfiguration;
    }
    //static long counter = 0;

    public void setQueryConfiguration(QueryConfiguration queryConfiguration) {
        this.queryConfiguration = queryConfiguration;
    }

    public void initializeTRangeQuery(QueryConfiguration conf){
        this.setQueryConfiguration(conf);
    }

    public abstract DataStream<? extends SpatialObject> run(DataStream<T> ordinaryStream, Set<K> objSet);

    public static DataStream<Point> realTimeNaive(Set<Polygon> polygonSet, DataStream<Point> pointStream){

        HashSet<String> polygonsGridCellIDs = new HashSet<>();
        // Making an integrated set of all the polygon's grid cell IDs
        for (Polygon poly: polygonSet) {
            polygonsGridCellIDs.addAll(poly.gridIDsSet);
        }

        // Perform keyBy to logically distribute streams by trajectoryID and then check if a point lies within a polygon or nor
        DataStream<Point> keyedStream =  pointStream.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
        }).filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point p) throws Exception {

                for (Polygon poly: polygonSet) {
                    //counter += 1;
                    //System.out.println("counter " +  counter);
                    if (poly.polygon.contains(p.point.getEnvelope())) { // Polygon contains the point
                        return true;
                    }
                }
                return false; // Polygon does not contain the point
            }
        });

        return keyedStream;
    }
}
