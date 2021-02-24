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

package GeoFlink.spatialOperators;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class JoinQuery implements Serializable {

    /*
    //--------------- GRID-BASED JOIN QUERY - POINT-POINT -----------------//
    public static DataStream<Tuple2<String, String>> SpatialJoinQuery(DataStream<Point> ordinaryPointStream, DataStream<Point> queryPointStream, double queryRadius, int windowSize, int slideStep, UniformGrid uGrid){

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedQueryStream(queryPointStream, queryRadius, uGrid);

        DataStream<Tuple2<String, String>> joinOutput = ordinaryPointStream.join(replicatedQueryStream)
                .where(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Point, Point, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> join(Point p, Point q) {
                        if (HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), q.point.getX(), q.point.getY()) <= queryRadius) {
                            return Tuple2.of(p.gridID, q.gridID);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- GRID-BASED JOIN QUERY - POINT-POLYGON -----------------//
    public static DataStream<Tuple2<String, String>> SpatialJoinQuery(DataStream<Polygon> polygonStream, DataStream<Point> queryPointStream, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep){

        DataStream<Point> replicatedQueryStream = JoinQuery.getReplicatedQueryStream(queryPointStream, queryRadius, uGrid);
        DataStream<Polygon> replicatedPolygonStream = polygonStream.flatMap(new HelperClass.ReplicatePolygonStream());

        DataStream<Tuple2<String, String>> joinOutput = replicatedPolygonStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point q) throws Exception {
                        return q.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Polygon, Point, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> join(Polygon poly, Point q) {
                        if (HelperClass.getPointPolygonBBoxMinEuclideanDistance(q, poly) <= queryRadius) {
                            return Tuple2.of(poly.gridID, q.gridID);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    //--------------- (MODIFIED) GRID-BASED JOIN QUERY - POINT-POLYGON -----------------//
    public static DataStream<Tuple2<String, String>> SpatialJoinQueryOptimized(DataStream<Polygon> polygonStream, DataStream<Point> queryPointStream, double queryRadius, UniformGrid uGrid, int windowSize, int slideStep){

        DataStream<Tuple2<Point,Boolean>> replicatedQueryStream = JoinQuery.getReplicatedQueryStreamModified(queryPointStream, queryRadius, uGrid);
        DataStream<Polygon> replicatedPolygonStream = polygonStream.flatMap(new HelperClass.ReplicatePolygonStream());

        DataStream<Tuple2<String, String>> joinOutput = replicatedPolygonStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Tuple2<Point,Boolean>, String>() {
                    @Override
                    public String getKey(Tuple2<Point,Boolean> q) throws Exception {
                        return q.f0.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Polygon, Tuple2<Point,Boolean>, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> join(Polygon poly, Tuple2<Point,Boolean> q) {
                        if (q.f1 == true) {  // guaranteed neighbors
                            return Tuple2.of(poly.gridID, q.f0.gridID);
                        } else { // candidate neighbors
                            if (HelperClass.getPointPolygonBBoxMinEuclideanDistance(q.f0, poly) <= queryRadius) {
                                return Tuple2.of(poly.gridID, q.f0.gridID);
                            } else {
                                return Tuple2.of(null, null);
                            }
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1 != null;
            }
        });
    }


    //--------------- GRID-BASED JOIN QUERY - POLYGON-POLYGON -----------------//
    public static DataStream<Tuple2<String,String>> SpatialJoinQuery(DataStream<Polygon> polygonStream, DataStream<Polygon> queryPolygonStream, int slideStep, int windowSize, double queryRadius, UniformGrid uGrid){
        DataStream<Polygon> replicatedQueryStream = JoinQuery.getReplicatedQueryStream(queryPolygonStream, uGrid, queryRadius);
        DataStream<Polygon> replicatedPolygonStream = polygonStream.flatMap(new HelperClass.ReplicatePolygonStream());

        DataStream<Tuple2<String, String>> joinOutput = replicatedPolygonStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon query) throws Exception {
                        return query.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Polygon, Polygon, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> join(Polygon poly, Polygon query) {
                        if (HelperClass.getBBoxBBoxMinEuclideanDistance(query.boundingBox, poly.boundingBox) <= queryRadius) {
                            return Tuple2.of(poly.gridID, query.gridID);
                        } else {
                            return Tuple2.of(null, null);
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1 != null;
            }
        });
    }

    //--------------- (MODIFIED) GRID-BASED JOIN QUERY - POLYGON-POLYGON -----------------//
    public static DataStream<Tuple2<String,String>> SpatialJoinQueryOptimized(DataStream<Polygon> polygonStream, DataStream<Polygon> queryPolygonStream, int slideStep, int windowSize, double queryRadius, UniformGrid uGrid){
        DataStream<Tuple2<Polygon,Boolean>> replicatedQueryStream = JoinQuery.getReplicatedQueryStreamModified(queryPolygonStream, uGrid, queryRadius);
        DataStream<Polygon> replicatedPolygonStream = polygonStream.flatMap(new HelperClass.ReplicatePolygonStream());

        DataStream<Tuple2<String, String>> joinOutput = replicatedPolygonStream.join(replicatedQueryStream)
                .where(new KeySelector<Polygon, String>() {
                    @Override
                    public String getKey(Polygon poly) throws Exception {
                        return poly.gridID;
                    }
                }).equalTo(new KeySelector<Tuple2<Polygon,Boolean>, String>() {
                    @Override
                    public String getKey(Tuple2<Polygon,Boolean> query) throws Exception {
                        return query.f0.gridID;
                    }
                }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                .apply(new JoinFunction<Polygon, Tuple2<Polygon,Boolean>, Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String, String> join(Polygon poly, Tuple2<Polygon,Boolean> query) {
                        if (query.f1 == true) {  // guaranteed neighbors
                            return Tuple2.of(poly.gridID, query.f0.gridID);
                        } else { // candidate neighbors
                            if (HelperClass.getBBoxBBoxMinEuclideanDistance(query.f0.boundingBox, poly.boundingBox) <= queryRadius) {
                                return Tuple2.of(poly.gridID, query.f0.gridID);
                            } else {
                                return Tuple2.of(null, null);
                            }
                        }
                    }
                });

        return joinOutput.filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1 != null;
            }
        });

    }





    //Replicate Query Point Stream for each Neighbouring Grid ID
    public static DataStream<Point> getReplicatedQueryStream(DataStream<Point> queryPoints, double queryRadius, UniformGrid uGrid){

        return queryPoints.flatMap(new FlatMapFunction<Point, Point>() {
            @Override
            public void flatMap(Point queryPoint, Collector<Point> out) throws Exception {

                // Neighboring cells contain all the cells including Candidate cells, Guaranteed Cells and the query point cell itself
                HashSet<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);

                // Create duplicated query points
                for (String gridID: neighboringCells) {
                    Point p = new Point(queryPoint.point.getX(), queryPoint.point.getY(), gridID);
                    out.collect(p);
                }
            }
        });
    }

    //Replicate Query Point Stream for each Candidate and Guaranteed Grid ID
    public static DataStream<Tuple2<Point,Boolean>> getReplicatedQueryStreamModified(DataStream<Point> queryPoints, double queryRadius, UniformGrid uGrid){

        return queryPoints.flatMap(new FlatMapFunction<Point, Tuple2<Point,Boolean>>() {
            @Override
            public void flatMap(Point queryPoint, Collector<Tuple2<Point,Boolean>> out) throws Exception {

                Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, queryPoint.gridID);
                Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, queryPoint.gridID, guaranteedNeighboringCells);

                // Create duplicated query points for Guaranteed Neighbors
                for (String gridID: guaranteedNeighboringCells) {
                    Point p = new Point(queryPoint.point.getX(), queryPoint.point.getY(), gridID);
                    out.collect(Tuple2.of(p,true));
                }

                // Create duplicated query points for Candidate Neighbors
                for (String gridID: candidateNeighboringCells) {
                    Point p = new Point(queryPoint.point.getX(), queryPoint.point.getY(), gridID);
                    out.collect(Tuple2.of(p,false));
                }
            }
        });
    }

    //Replicate Query Polygon Stream for each Neighbouring Grid ID
    public static DataStream<Polygon> getReplicatedQueryStream(DataStream<Polygon> queryPolygons, UniformGrid uGrid, double queryRadius){
        return queryPolygons.flatMap(new RichFlatMapFunction<Polygon, Polygon>() {
            private long parallelism;
            private int uniqueObjID;

            @Override
            public void open(Configuration parameters) {
                RuntimeContext ctx = getRuntimeContext();
                parallelism = ctx.getNumberOfParallelSubtasks();
                uniqueObjID = ctx.getIndexOfThisSubtask();
            }

            @Override
            public void flatMap(Polygon poly, Collector<Polygon> out) throws Exception {
                Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, poly);
                Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, poly, guaranteedNeighboringCells);

                // Create duplicated polygon stream for all neighbouring cells based on GridIDs
                for (String gridID: guaranteedNeighboringCells) {
                    Polygon p = new Polygon(Arrays.asList(poly.getCoordinates()), uniqueObjID, poly.gridIDsSet, gridID, poly.boundingBox);
                    out.collect(p);
                }
                for (String gridID: candidateNeighboringCells) {
                    Polygon p = new Polygon(Arrays.asList(poly.getCoordinates()), uniqueObjID, poly.gridIDsSet, gridID, poly.boundingBox);
                    out.collect(p);
                }

                // Generating unique ID for each polygon, so that all the replicated tuples are assigned the same unique id
                uniqueObjID += parallelism;
            }
        });
    }

    //Replicate Query Polygon Stream for each Candidate and Guaranteed Grid ID
    public static DataStream<Tuple2<Polygon,Boolean>> getReplicatedQueryStreamModified(DataStream<Polygon> queryPolygons, UniformGrid uGrid, double queryRadius){
        return queryPolygons.flatMap(new RichFlatMapFunction<Polygon, Tuple2<Polygon,Boolean>>() {
            private long parallelism;
            private int uniqueObjID;

            @Override
            public void open(Configuration parameters) {
                RuntimeContext ctx = getRuntimeContext();
                parallelism = ctx.getNumberOfParallelSubtasks();
                uniqueObjID = ctx.getIndexOfThisSubtask();
            }

            @Override
            public void flatMap(Polygon poly, Collector<Tuple2<Polygon,Boolean>> out) throws Exception {
                Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, poly);
                Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, poly, guaranteedNeighboringCells);

                // Create duplicated polygon stream based on GridIDs
                for (String gridID: guaranteedNeighboringCells) {
                    Polygon p = new Polygon(Arrays.asList(poly.getCoordinates()), uniqueObjID, poly.gridIDsSet, gridID, poly.boundingBox);
                    out.collect(Tuple2.of(p,true));
                }
                for (String gridID: candidateNeighboringCells) {
                    Polygon p = new Polygon(Arrays.asList(poly.getCoordinates()), uniqueObjID, poly.gridIDsSet, gridID, poly.boundingBox);
                    out.collect(Tuple2.of(p,false));
                }

                // Generating unique ID for each polygon, so that all the replicated tuples are assigned the same unique id
                uniqueObjID += parallelism;
            }
        });
    }
     */
}


