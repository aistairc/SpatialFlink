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

public class PointPointPolygonTRangeQuery extends TRangeQuery<Point, Point, Polygon> {
    public PointPointPolygonTRangeQuery(QueryConfiguration conf) {
        super.initializeTRangeQuery(conf);
    }

    public DataStream<Point> run(DataStream<Point> pointStream, Set<Polygon> polygonSet) {
        //--------------- Real-time - POINT - POINT - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            return realTime(pointStream, polygonSet);
        }

        //--------------- Window-based - POINT - POLYGON -----------------//
        else if(this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            throw new IllegalArgumentException("Not yet support");
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Point> realTime(DataStream<Point> pointStream, Set<Polygon> polygonSet) {

        HashSet<String> polygonsGridCellIDs = new HashSet<>();
        // Making an integrated set of all the polygon's grid cell IDs
        for (Polygon poly : polygonSet) {
            polygonsGridCellIDs.addAll(poly.gridIDsSet);
        }

        // Filtering based on grid-cell ID
        DataStream<Point> filteredStream = pointStream.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point p) throws Exception {
                return ((polygonsGridCellIDs.contains(p.gridID)));
            }
        });

        // Perform keyBy to logically distribute streams by trajectoryID and then check if a point lies within a polygon or nor
        return filteredStream.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
        }).filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point p) throws Exception {
                for (Polygon poly : polygonSet) {
                    if (poly.polygon.contains(p.point.getEnvelope())) // Polygon contains the point
                        return true;
                }
                return false; // Polygon does not contain the point
            }
        });
    }
}