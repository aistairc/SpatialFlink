package GeoFlink.spatialOperators.tjoin;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.spatialOperators.QueryConfiguration;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

public abstract class TJoinQuery<T extends SpatialObject, K extends SpatialObject> implements Serializable {
    private QueryConfiguration queryConfiguration;
    private SpatialIndex spatialIndex;

    public QueryConfiguration getQueryConfiguration() {
        return queryConfiguration;
    }

    public void setQueryConfiguration(QueryConfiguration queryConfiguration) {
        this.queryConfiguration = queryConfiguration;
    }

    public SpatialIndex getSpatialIndex() {
        return spatialIndex;
    }

    public void setSpatialIndex(SpatialIndex spatialIndex) {
        this.spatialIndex = spatialIndex;
    }

    public void initializeKNNQuery(QueryConfiguration conf, SpatialIndex index){
        this.setQueryConfiguration(conf);
        this.setSpatialIndex(index);
    }

    public abstract DataStream<Tuple2<T, T>> run(DataStream<K> ordinaryStream, DataStream<K> queryStream, double joinDistance);

    // User Defined Classes
    // Key selector
    protected class trajIDKeySelector implements KeySelector<Point,String> {
        @Override
        public String getKey(Point p) throws Exception {
            return p.objID; // trajectory id
        }
    }

    protected class GenerateWindowedTrajectory extends RichWindowFunction<Point, LineString, String, TimeWindow> {

        //ctor
        public  GenerateWindowedTrajectory() {};

        @Override
        public void apply(String trajID, TimeWindow timeWindow, Iterable<Point> input, Collector<LineString> trajectory) throws Exception {

            List<Coordinate> coordinateList = new LinkedList<>();
            HashSet<String> gridIDsSet = new HashSet<>();

            coordinateList.clear();
            gridIDsSet.clear();

            for (Point p : input) {
                coordinateList.add(new Coordinate(p.point.getX(), p.point.getY()));
                gridIDsSet.add(p.gridID);
            }

            if (coordinateList.size() > 1 ) { // At least two points are required for a lineString construction
                LineString ls = new LineString(trajID, coordinateList, gridIDsSet);

                if (ls != null) {
                    trajectory.collect(ls);
                }
            }
        }
    }

    //Replicate Query Point Stream for each Neighbouring Grid ID
    protected DataStream<Point> getReplicatedQueryStream(DataStream<Point> queryPoints, double queryRadius, UniformGrid uGrid){

        return queryPoints.flatMap(new FlatMapFunction<Point, Point>() {
            @Override
            public void flatMap(Point queryPoint, Collector<Point> out) throws Exception {

                // Neighboring cells contain all the cells including Candidate cells, Guaranteed Cells and the query point cell itself
                HashSet<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);

                // Create duplicated query points
                for (String gridID: neighboringCells) {
                    Point p = new Point(queryPoint.objID, queryPoint.point.getX(), queryPoint.point.getY(), queryPoint.timeStampMillisec, gridID);
                    out.collect(p);
                }
            }
        });
    }
}
