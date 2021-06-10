package GeoFlink.spatialOperators.trange;

import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.HashSet;
import java.util.Set;

public class PointPolygonPointTRangeQuery extends TRangeQuery<Point, Polygon, Point> {
    public PointPolygonPointTRangeQuery(QueryConfiguration conf) {
        super.initializeTRangeQuery(conf);
    }

    public DataStream<Point> run(DataStream<Polygon> polygonStream, Set<Point> pointSet) {
        //--------------- Real-time - POINT - POLYGON - POINT -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            throw new IllegalArgumentException("Not yet support");
        }

        //--------------- Window-based - POINT - POLYGON -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased)
        {
            throw new IllegalArgumentException("Not yet support");
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    public DataStream<Point> runNative(Set<Polygon> polygonSet, DataStream<Point> pointStream) {
        //--------------- Real-time - POINT - POINT - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            return realTimeNative(polygonSet, pointStream);
        }

        //--------------- Window-based - POINT - POLYGON -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            throw new IllegalArgumentException("Not yet support");
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME Native
    private DataStream<Point> realTimeNative(Set<Polygon> polygonSet, DataStream<Point> pointStream){

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
                    if (poly.polygon.contains(p.point.getEnvelope())) // Polygon contains the point
                        return true;
                }
                return false; // Polygon does not contain the point
            }
        });

        return keyedStream;
    }
}
