/*
Copyright 2020 Data Platform Research Team, AIRC, AIST, Japan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package GeoFlink.spatialOperators.join;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.SpatialOperator;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

public abstract class JoinQuery<T extends SpatialObject, K extends SpatialObject> extends SpatialOperator implements Serializable {
    private QueryConfiguration queryConfiguration;
    private SpatialIndex spatialIndex1;
    private SpatialIndex spatialIndex2;

    public QueryConfiguration getQueryConfiguration() {
        return queryConfiguration;
    }

    public void setQueryConfiguration(QueryConfiguration queryConfiguration) {
        this.queryConfiguration = queryConfiguration;
    }

    public SpatialIndex getSpatialIndex1() {
        return spatialIndex1;
    }

    public void setSpatialIndex1(SpatialIndex spatialIndex) {
        this.spatialIndex1 = spatialIndex;
    }

    public SpatialIndex getSpatialIndex2() {
        return spatialIndex2;
    }

    public void setSpatialIndex2(SpatialIndex spatialIndex) {
        this.spatialIndex2 = spatialIndex;
    }

    public void initializeJoinQuery(QueryConfiguration conf, SpatialIndex index1, SpatialIndex index2) {
        this.setQueryConfiguration(conf);
        this.setSpatialIndex1(index1);
        this.setSpatialIndex2(index2);
    }

    public abstract DataStream<Tuple2<T, K>> run(DataStream<T> ordinaryStream, DataStream<K> queryStream, double queryRadius);

    //Replicate Query Point Stream for each Neighbouring Grid ID
    public static DataStream<Point> getReplicatedPointQueryStream(DataStream<Point> queryPoints, double queryRadius, UniformGrid uGrid){

        return queryPoints.flatMap(new FlatMapFunction<Point, Point>() {
            @Override
            public void flatMap(Point queryPoint, Collector<Point> out) throws Exception {

                // Neighboring cells contain all the cells including Candidate cells, Guaranteed Cells and the query point cell itself
                HashSet<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);

                // Create duplicated query points
                for (String gridID: neighboringCells) {
                    //Point p = new Point(queryPoint.point.getX(), queryPoint.point.getY(), gridID);
                    Point p = new Point(queryPoint.objID, queryPoint.point.getX(), queryPoint.point.getY(), queryPoint.timeStampMillisec, gridID);
                    out.collect(p);
                }
            }
        });
    }

    //Replicate Query Polygon Stream for each Neighbouring Grid ID
    public static DataStream<Polygon> getReplicatedPolygonQueryStream(DataStream<Polygon> queryPolygons, double queryRadius, UniformGrid uGrid){

        return queryPolygons.flatMap(new RichFlatMapFunction<Polygon, Polygon>() {

            @Override
            public void flatMap(Polygon poly, Collector<Polygon> out) throws Exception {
                Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, poly);
                Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, poly, guaranteedNeighboringCells);

                // Create duplicated polygon stream for all neighbouring cells based on GridIDs
                for (String gridID: guaranteedNeighboringCells) {
                    //Polygon p = new Polygon(poly.getCoordinates(), uniqueObjID, poly.gridIDsSet, gridID, poly.timeStampMillisec, poly.boundingBox);
                    Polygon p = new Polygon(poly.getCoordinates(), poly.objID, poly.gridIDsSet, gridID, poly.timeStampMillisec, poly.boundingBox);
                    out.collect(p);
                }
                for (String gridID: candidateNeighboringCells) {
                    //Polygon p = new Polygon(poly.getCoordinates(), uniqueObjID, poly.gridIDsSet, gridID, poly.timeStampMillisec, poly.boundingBox);
                    Polygon p = new Polygon(poly.getCoordinates(), poly.objID, poly.gridIDsSet, gridID, poly.timeStampMillisec, poly.boundingBox);
                    out.collect(p);
                }
            }
        });
    }

    public static DataStream<LineString> getReplicatedLineStringQueryStream(DataStream<LineString> queryLineString, double queryRadius, UniformGrid uGrid){

        return queryLineString.flatMap(new RichFlatMapFunction<LineString, LineString>() {

            @Override
            public void flatMap(LineString lineString, Collector<LineString> out) throws Exception {
                Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, lineString);
                Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, lineString, guaranteedNeighboringCells);

                // Create duplicated polygon stream for all neighbouring cells based on GridIDs
                for (String gridID: guaranteedNeighboringCells) {
                    LineString ls = new LineString(lineString.objID, Arrays.asList(lineString.lineString.getCoordinates().clone()), lineString.gridIDsSet, gridID, lineString.timeStampMillisec, lineString.boundingBox);
                    out.collect(ls);
                }
                for (String gridID: candidateNeighboringCells) {
                    LineString ls = new LineString(lineString.objID, Arrays.asList(lineString.lineString.getCoordinates().clone()), lineString.gridIDsSet, gridID, lineString.timeStampMillisec, lineString.boundingBox);
                    out.collect(ls);
                }
            }
        });
    }
}