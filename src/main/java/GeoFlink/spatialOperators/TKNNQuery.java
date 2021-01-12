package GeoFlink.spatialOperators;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.Comparators;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;


import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

public class TKNNQuery implements Serializable {

    //--------------- TKNNQuery - Real-time -----------------//
    public static DataStream<Tuple3<String, LineString, Double>> TSpatialKNNQuery(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k, int windowSize, int windowSlideStep, UniformGrid uGrid) {

        Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                return (neighboringCells.contains(point.gridID));
            }
        });

        // Output objID and its distance from point p
        DataStream<Tuple2<String, Double>> windowedKNN = filteredPoints.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.gridID;
            }
        }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Point, Tuple2<String, Double>, String, TimeWindow>() {


                    Map<String, Double> objMap = new HashMap<String, Double>();
                    HashMap<String, Double> sortedObjMap = new LinkedHashMap<>();

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<Tuple2<String, Double>> outputStream) throws Exception {

                        objMap.clear();
                        sortedObjMap.clear();

                        // compute the distance of all trajectory points w.r.t. query point and return the kNN (trajectory ID, distance) pairs
                        for (Point p : inputTuples) {

                            Double newDistance = HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
                            Double existingDistance = objMap.get(p.objID);

                            if (existingDistance == null) { // if object with the given ObjID does not already exist
                                objMap.put(p.objID, newDistance);
                            } else { // object already exist
                                if (newDistance < existingDistance)
                                    objMap.replace(p.objID, newDistance);
                            }
                        }

                        // Sorting the map
                        List<Map.Entry<String, Double>> list = new LinkedList<>(objMap.entrySet());
                        Collections.sort(list, Comparator.comparing(o -> o.getValue()));

                        for (Map.Entry<String, Double> map : list) {
                            sortedObjMap.put(map.getKey(), map.getValue());
                        }

                        // Logic to return the kNN (trajectory ID, distance) pairs
                        int counter = 0;
                        for (Map.Entry<String, Double> entry : sortedObjMap.entrySet()) {
                            if (counter == k) break;
                            outputStream.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                            counter++;
                        }
                    }
                });


        // The join causes the non-kNN to filter out and outputs a tuple of <objID, Point, DistanceFromQueryPoint>
        DataStream<Tuple3<String, Point, Double>> windowedJoindKNN = windowedKNN.join(pointStreamWithTsAndWm)
                .where(new KeySelector<Tuple2<String, Double>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Double> e) throws Exception {
                        return e.f0;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.objID;
                    }
                }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new JoinFunction<Tuple2<String, Double>, Point, Tuple3<String, Point, Double>>() {
                    @Override
                    public Tuple3<String, Point, Double> join(Tuple2<String, Double> first, Point second) throws Exception {
                        return Tuple3.of(first.f0, second, first.f1);
                    }
                }).filter(new FilterFunction<Tuple3<String, Point, Double>>() {
                    @Override
                    public boolean filter(Tuple3<String, Point, Double> value) throws Exception {
                        return value.f0 != null && value.f1 != null;
                    }
                });


        // Logic to integrate all the kNNs to produce an integrated kNN
        return windowedJoindKNN.windowAll(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new AllWindowFunction<Tuple3<String, Point, Double>, Tuple3<String, LineString, Double>, TimeWindow>() {

                    //Map of objID and LineString
                    Map<String, List<Coordinate>> trajectories = new HashMap<>();
                    Map<String, Double> objDistFromQueryPoint = new HashMap<>();

                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple3<String, Point, Double>> input, Collector<Tuple3<String, LineString, Double>> output) throws Exception {

                        //org.locationtech.jts.geom.LineString lineString = geofact.createLineString(coordinates.toArray(new Coordinate[0]));


                        trajectories.clear();
                        objDistFromQueryPoint.clear();

                        for (Tuple3<String, Point, Double> e : input) {

                            List<Coordinate> coordinateListIf = new ArrayList<Coordinate>();
                            if ((coordinateListIf = trajectories.get(e.f0)) != null) // if trajectory exist
                            {
                                // Updating trajectory
                                coordinateListIf.add(new Coordinate(e.f1.point.getX(), e.f1.point.getY()));
                                trajectories.replace(e.f0, coordinateListIf);

                                // Updating the object distance
                                Double existingTrajDist = objDistFromQueryPoint.get(e.f0);
                                if(existingTrajDist > e.f2)
                                    objDistFromQueryPoint.replace(e.f0, e.f2);

                            } else // Create a lineString with one point if does not exist already
                            {
                                List<Coordinate> coordinateListElse = new ArrayList<Coordinate>();
                                coordinateListElse.add(new Coordinate(e.f1.point.getX(), e.f1.point.getY()));
                                // Inserting trajectory
                                trajectories.put(e.f0, coordinateListElse);
                                // Inserting traj distance
                                objDistFromQueryPoint.put(e.f0, e.f2);
                            }
                        }

                        // Sorting the objDistFromQueryPoint map by value
                        HashMap<String, Double> sortedobjDistFromQueryPoint = new LinkedHashMap<>();
                        List<Map.Entry<String, Double>> list = new LinkedList<>(objDistFromQueryPoint.entrySet());
                        Collections.sort(list, Comparator.comparing(o -> o.getValue()));

                        for (Map.Entry<String, Double> map : list) {
                            sortedobjDistFromQueryPoint.put(map.getKey(), map.getValue());
                        }

                        // Logic to return the kNN (trajectory ID, distance) pairs
                        int counter = 0;
                        for (Map.Entry<String, Double> entry : sortedobjDistFromQueryPoint.entrySet()) {
                            if (counter == k) break; // to guarantee that only k outputs are generated

                            List<Coordinate> coordinateList = trajectories.get(entry.getKey());
                            if(coordinateList.size() > 1) { // for linestring creation, at-least 2 points are required
                                LineString ls = new LineString(entry.getKey(), coordinateList, uGrid);
                                output.collect(Tuple3.of(entry.getKey(), ls, entry.getValue()));
                                counter++;
                            }
                        }
                    }
                });
    }


    //--------------- TKNNQuery - Naive -----------------//
    public static DataStream<Tuple3<String, LineString, Double>> TSpatialKNNQuery(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k, int windowSize, int windowSlideStep) {


        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(windowSize)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        // Output objID and its distance from point p
        DataStream<Tuple2<String, Double>> windowedKNN = pointStreamWithTsAndWm.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.gridID;
            }
        }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Point, Tuple2<String, Double>, String, TimeWindow>() {


                    Map<String, Double> objMap = new HashMap<String, Double>();
                    HashMap<String, Double> sortedObjMap = new LinkedHashMap<>();

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<Tuple2<String, Double>> outputStream) throws Exception {

                        objMap.clear();
                        sortedObjMap.clear();

                        // compute the distance of all trajectory points w.r.t. query point and return the kNN (trajectory ID, distance) pairs
                        for (Point p : inputTuples) {

                            Double newDistance = HelperClass.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
                            Double existingDistance = objMap.get(p.objID);

                            if (existingDistance == null) { // if object with the given ObjID does not already exist
                                objMap.put(p.objID, newDistance);
                            } else { // object already exist
                                if (newDistance < existingDistance)
                                    objMap.replace(p.objID, newDistance);
                            }
                        }

                        // Sorting the map
                        List<Map.Entry<String, Double>> list = new LinkedList<>(objMap.entrySet());
                        Collections.sort(list, Comparator.comparing(o -> o.getValue()));

                        for (Map.Entry<String, Double> map : list) {
                            sortedObjMap.put(map.getKey(), map.getValue());
                        }

                        // Logic to return the kNN (trajectory ID, distance) pairs
                        int counter = 0;
                        for (Map.Entry<String, Double> entry : sortedObjMap.entrySet()) {
                            if (counter == k) break;
                            outputStream.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                            counter++;
                        }
                    }
                });


        // The join causes the non-kNN to filter out and outputs a tuple of <objID, Point, DistanceFromQueryPoint>
        DataStream<Tuple3<String, Point, Double>> windowedJoindKNN = windowedKNN.join(pointStreamWithTsAndWm)
                .where(new KeySelector<Tuple2<String, Double>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Double> e) throws Exception {
                        return e.f0;
                    }
                }).equalTo(new KeySelector<Point, String>() {
                    @Override
                    public String getKey(Point p) throws Exception {
                        return p.objID;
                    }
                }).window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new JoinFunction<Tuple2<String, Double>, Point, Tuple3<String, Point, Double>>() {
                    @Override
                    public Tuple3<String, Point, Double> join(Tuple2<String, Double> first, Point second) throws Exception {
                        return Tuple3.of(first.f0, second, first.f1);
                    }
                }).filter(new FilterFunction<Tuple3<String, Point, Double>>() {
                    @Override
                    public boolean filter(Tuple3<String, Point, Double> value) throws Exception {
                        return value.f0 != null && value.f1 != null;
                    }
                });


        // Logic to integrate all the kNNs to produce an integrated kNN
        return windowedJoindKNN.windowAll(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new AllWindowFunction<Tuple3<String, Point, Double>, Tuple3<String, LineString, Double>, TimeWindow>() {

                    //Map of objID and LineString
                    Map<String, List<Coordinate>> trajectories = new HashMap<>();
                    Map<String, Double> objDistFromQueryPoint = new HashMap<>();

                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple3<String, Point, Double>> input, Collector<Tuple3<String, LineString, Double>> output) throws Exception {

                        //org.locationtech.jts.geom.LineString lineString = geofact.createLineString(coordinates.toArray(new Coordinate[0]));


                        trajectories.clear();
                        objDistFromQueryPoint.clear();

                        for (Tuple3<String, Point, Double> e : input) {

                            List<Coordinate> coordinateListIf = new ArrayList<Coordinate>();
                            if ((coordinateListIf = trajectories.get(e.f0)) != null) // if trajectory exist
                            {
                                // Updating trajectory
                                coordinateListIf.add(new Coordinate(e.f1.point.getX(), e.f1.point.getY()));
                                trajectories.replace(e.f0, coordinateListIf);

                                // Updating the object distance
                                Double existingTrajDist = objDistFromQueryPoint.get(e.f0);
                                if(existingTrajDist > e.f2)
                                    objDistFromQueryPoint.replace(e.f0, e.f2);

                            } else // Create a lineString with one point if does not exist already
                            {
                                List<Coordinate> coordinateListElse = new ArrayList<Coordinate>();
                                coordinateListElse.add(new Coordinate(e.f1.point.getX(), e.f1.point.getY()));
                                // Inserting trajectory
                                trajectories.put(e.f0, coordinateListElse);
                                // Inserting traj distance
                                objDistFromQueryPoint.put(e.f0, e.f2);
                            }
                        }

                        // Sorting the objDistFromQueryPoint map by value
                        HashMap<String, Double> sortedobjDistFromQueryPoint = new LinkedHashMap<>();
                        List<Map.Entry<String, Double>> list = new LinkedList<>(objDistFromQueryPoint.entrySet());
                        Collections.sort(list, Comparator.comparing(o -> o.getValue()));

                        for (Map.Entry<String, Double> map : list) {
                            sortedobjDistFromQueryPoint.put(map.getKey(), map.getValue());
                        }

                        // Logic to return the kNN (trajectory ID, distance) pairs
                        int counter = 0;
                        for (Map.Entry<String, Double> entry : sortedobjDistFromQueryPoint.entrySet()) {
                            if (counter == k) break; // to guarantee that only k outputs are generated

                            List<Coordinate> coordinateList = trajectories.get(entry.getKey());
                            if(coordinateList.size() > 1) { // for linestring creation, at-least 2 points are required
                                LineString ls = new LineString(entry.getKey(), coordinateList);
                                output.collect(Tuple3.of(entry.getKey(), ls, entry.getValue()));
                                counter++;
                            }
                        }
                    }
                });
    }

}