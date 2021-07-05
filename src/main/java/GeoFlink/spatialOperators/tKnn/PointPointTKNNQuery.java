package GeoFlink.spatialOperators.tKnn;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.DistanceFunctions;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.util.*;

public class PointPointTKNNQuery extends TKNNQuery<Point, Point> {
    public PointPointTKNNQuery(QueryConfiguration conf, SpatialIndex index) {
        super.initializeTKNNQuery(conf, index);
    }

    public DataStream<?> run(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k) {
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();

        //--------------- Real-time - POINT - POINT -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realtime(pointStream, queryPoint, queryRadius, k, omegaJoinDurationSeconds, allowedLateness, uGrid);
        }

        //--------------- Window-based - POINT - POINT -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int windowSlideStep = this.getQueryConfiguration().getSlideStep();
            return windowBased(pointStream, queryPoint, queryRadius, k, windowSize, windowSlideStep, allowedLateness, uGrid);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    public DataStream<?> runNaive(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k) {
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        //--------------- Real-time - POINT - POINT -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            int omegaJoinDurationSeconds = this.getQueryConfiguration().getWindowSize();
            return realTimeNaive(pointStream, queryPoint, queryRadius, k, omegaJoinDurationSeconds, allowedLateness);
        }

        //--------------- Window-based - POINT - POINT -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int windowSlideStep = this.getQueryConfiguration().getSlideStep();
            return windowBasedNaive(pointStream, queryPoint, k, windowSize, windowSlideStep, allowedLateness);
        }

        else {
            throw new IllegalArgumentException("Not yet support");
        }
    }

    // REAL-TIME
    private DataStream<Tuple2<Point, Double>> realtime(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k, int omegaDuration, int allowedLateness, UniformGrid uGrid) {

        Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                }).startNewChain();

        DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                //return (neighboringCells.contains(point.gridID));
                if(neighboringCells.contains(point.gridID)){ // Cell-based pruning
                    //dCounter += 1;
                    //System.out.println("counter " +  dCounter);

                    if(DistanceFunctions.getPointPointEuclideanDistance(point.point.getX(), point.point.getY(), queryPoint.point.getX(), queryPoint.point.getY()) <= queryRadius)
                        return true;
                    else
                        return false;
                }
                else
                    return false;
            }
        });

        //filteredPoints.print();
        // Output at-most k objIDs and their distances from point p
        DataStream<Tuple2<Point, Double>> kNNStream = filteredPoints.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                //return p.gridID;
                return p.objID;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(omegaDuration)))
                .trigger(new TKNNQuery.realTimeWindowTrigger())
                .apply(new TKNNQuery.kNNEvaluationRealtime(queryPoint, k));

        // Logic to integrate all the kNNs to produce an integrated kNN
        return kNNStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(omegaDuration)))
                .trigger(new TKNNQuery.realTimeWindowAllTrigger())
                .apply(new AllWindowFunction<Tuple2<Point, Double>, Tuple2<Point, Double>, TimeWindow>() {

                    //Map of objID and Point
                    Map<String, Point> pointsIDMap = new HashMap<>();
                    //Map of objID and distFromQueryPoint
                    Map<String, Double> pointDistFromQueryPoint = new HashMap<>();
                    //Map for sorting distFromQueryPoint
                    HashMap<String, Double> sortedPointDistFromQueryPoint = new LinkedHashMap<>();

                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<Point, Double>> input, Collector<Tuple2<Point, Double>> output) throws Exception {

                        pointsIDMap.clear();
                        pointDistFromQueryPoint.clear();
                        sortedPointDistFromQueryPoint.clear();

                        // Collect all the points in two maps
                        for (Tuple2<Point, Double> e : input) {



                            Double existingDistance = pointDistFromQueryPoint.get(e.f0.objID);
                            // Check if a point already exist, if not insert it, else replace previous point if the current point distance is less than previous point with the same ID
                            if (existingDistance == null) { // if object with the given ObjID does not already exist
                                pointDistFromQueryPoint.put(e.f0.objID, e.f1);
                                pointsIDMap.put(e.f0.objID, e.f0);
                            } else { // object already exist
                                if (e.f1 < existingDistance) {
                                    pointDistFromQueryPoint.replace(e.f0.objID, e.f1);
                                    pointsIDMap.replace(e.f0.objID, e.f0);
                                }
                            }
                        }

                        // Sorting the pointDistFromQueryPoint map by value
                        List<Map.Entry<String, Double>> list = new LinkedList<>(pointDistFromQueryPoint.entrySet());
                        Collections.sort(list, Comparator.comparing(o -> o.getValue()));
                        for (Map.Entry<String, Double> map : list) {
                            sortedPointDistFromQueryPoint.put(map.getKey(), map.getValue());
                        }

                        // Logic to return the kNN (trajectory ID, distance) pairs
                        int counter = 0;
                        for (Map.Entry<String, Double> entry: sortedPointDistFromQueryPoint.entrySet()) {
                            if (counter == k) break; // to guarantee that only k outputs are generated
                            output.collect(Tuple2.of(pointsIDMap.get(entry.getKey()), entry.getValue()));
                            counter++;
                        }
                    }
                });
    }

    // WINDOW BASED
    private DataStream<Tuple2<LineString, Double>> windowBased(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k, int windowSize, int windowSlideStep, int allowedLateness, UniformGrid uGrid) {

        Set<String> neighboringCells = uGrid.getNeighboringCells(queryRadius, queryPoint);

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
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
                .apply(new TKNNQuery.kNNEvaluationWindowed(queryPoint, k));


        // Logic to generate sub-trajectories using original non-filtered input stream
        // The join causes the non-kNN to filter out and outputs a tuple of <objID, Point, DistanceFromQueryPoint>
        // KeyBy is performed on objID (trajectoryID), which will cause only the points corresponding to a single trajectory to be processed by a single operator instance
        DataStream<Tuple2<Point, Double>> windowedJoindKNN = windowedKNN.join(pointStreamWithTsAndWm)
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
                .apply(new JoinFunction<Tuple2<String, Double>, Point, Tuple2<Point, Double>>() {
                    @Override
                    public Tuple2<Point, Double> join(Tuple2<String, Double> first, Point second) throws Exception {
                        return Tuple2.of(second, first.f1);
                    }
                }).filter(new FilterFunction<Tuple2<Point, Double>>() {
                    @Override
                    public boolean filter(Tuple2<Point, Double> value) throws Exception {
                        return value.f0 != null;
                    }
                });


        // Construct window based sub-trajectories
        DataStream<Tuple2<LineString, Double>> windowedJoindKNNTraj = windowedJoindKNN.keyBy(new KeySelector<Tuple2<Point, Double>, String>() {
            @Override
            public String getKey(Tuple2<Point, Double> e) throws Exception {
                return e.f0.objID;
            }
        }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new WindowFunction<Tuple2<Point, Double>, Tuple2<LineString, Double>, String, TimeWindow>() {
                    List<Coordinate> coordinateList = new LinkedList<>();
                    @Override
                    public void apply(String objID, TimeWindow timeWindow, Iterable<Tuple2<Point, Double>> pointIterator, Collector<Tuple2<LineString, Double>> trajectory) throws Exception {
                        coordinateList.clear();
                        Double trajDistFromQ = 0.0;
                        Boolean setTrajDistFromQ = false;

                        for (Tuple2<Point, Double> p : pointIterator) {
                            coordinateList.add(new Coordinate(p.f0.point.getX(), p.f0.point.getY()));

                            // If not already set trajDistFromQ, set it
                            if(!setTrajDistFromQ){
                                trajDistFromQ = p.f1;
                                setTrajDistFromQ = true;
                            }
                        }
                        // At least two points are required for a lineString construction
                        if(coordinateList.size() > 1) {
                            LineString ls = new LineString(objID, coordinateList);
                            trajectory.collect(Tuple2.of(ls, trajDistFromQ));
                        }
                    }
                });


        // Logic to integrate all the kNNs to produce an integrated kNN
        return windowedJoindKNNTraj.windowAll(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(windowSlideStep)))
                .apply(new AllWindowFunction<Tuple2<LineString, Double>, Tuple2<LineString, Double>, TimeWindow>() {

                    //Map of objID and LineString
                    Map<String, LineString> trajectories = new HashMap<>();
                    //Map of objID and distFromQueryPoint
                    Map<String, Double> trajDistFromQueryPoint = new HashMap<>();

                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<LineString, Double>> input, Collector<Tuple2<LineString, Double>> output) throws Exception {

                        trajectories.clear();
                        trajDistFromQueryPoint.clear();

                        // Collect all the sub-trajectories in two maps
                        for (Tuple2<LineString, Double> e : input) {
                            trajectories.put(e.f0.objID, e.f0);
                            trajDistFromQueryPoint.put(e.f0.objID, e.f1);
                        }

                        // Sorting the trajDistFromQueryPoint map by value
                        HashMap<String, Double> sortedTrajDistFromQueryPoint = new LinkedHashMap<>();
                        List<Map.Entry<String, Double>> list = new LinkedList<>(trajDistFromQueryPoint.entrySet());
                        Collections.sort(list, Comparator.comparing(o -> o.getValue()));
                        for (Map.Entry<String, Double> map : list) {
                            sortedTrajDistFromQueryPoint.put(map.getKey(), map.getValue());
                        }

                        // Logic to return the kNN (trajectory ID, distance) pairs
                        int counter = 0;
                        for (Map.Entry<String, Double> entry: sortedTrajDistFromQueryPoint.entrySet()) {
                            if (counter == k) break; // to guarantee that only k outputs are generated
                            output.collect(Tuple2.of(trajectories.get(entry.getKey()), entry.getValue()));
                            counter++;
                        }
                    }
                });


        /*
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
         */
    }

    // REAL-TIME Naive
    private DataStream<Tuple2<Point, Double>> realTimeNaive(DataStream<Point> pointStream, Point queryPoint, double queryRadius, Integer k, int omegaDuration, int allowedLateness) {

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
                    @Override
                    public long extractTimestamp(Point p) {
                        return p.timeStampMillisec;
                    }
                });

        DataStream<Point> filteredPoints = pointStreamWithTsAndWm.filter(new FilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {

                //dCounter += 1;
                //System.out.println("counter " +  dCounter);

                if(DistanceFunctions.getPointPointEuclideanDistance(point.point.getX(), point.point.getY(), queryPoint.point.getX(), queryPoint.point.getY()) <= queryRadius)
                    return true;
                else
                    return false;
            }
        }).startNewChain();

        DataStream<Tuple2<Point, Double>> kNNStream = filteredPoints.keyBy(new KeySelector<Point, String>() {
            @Override
            public String getKey(Point p) throws Exception {
                return p.objID;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(omegaDuration)))
            .trigger(new TKNNQuery.realTimeWindowTrigger())
            .apply(new TKNNQuery.kNNEvaluationRealtime(queryPoint, k));

        // Logic to integrate all the kNNs to produce an integrated kNN
        return kNNStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(omegaDuration)))
                .trigger(new TKNNQuery.realTimeWindowAllTrigger())
                .apply(new AllWindowFunction<Tuple2<Point, Double>, Tuple2<Point, Double>, TimeWindow>() {

                    //Map of objID and Point
                    Map<String, Point> pointsIDMap = new HashMap<>();
                    //Map of objID and distFromQueryPoint
                    Map<String, Double> pointDistFromQueryPoint = new HashMap<>();
                    //Map for sorting distFromQueryPoint
                    HashMap<String, Double> sortedPointDistFromQueryPoint = new LinkedHashMap<>();

                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple2<Point, Double>> input, Collector<Tuple2<Point, Double>> output) throws Exception {

                        pointsIDMap.clear();
                        pointDistFromQueryPoint.clear();
                        sortedPointDistFromQueryPoint.clear();

                        // Collect all the points in two maps
                        for (Tuple2<Point, Double> e : input) {

                            Double existingDistance = pointDistFromQueryPoint.get(e.f0.objID);
                            // Check if a point already exist, if not insert it, else replace previous point if the current point distance is less than previous point with the same ID
                            if (existingDistance == null) { // if object with the given ObjID does not already exist
                                pointDistFromQueryPoint.put(e.f0.objID, e.f1);
                                pointsIDMap.put(e.f0.objID, e.f0);
                            } else { // object already exist
                                if (e.f1 < existingDistance) {
                                    pointDistFromQueryPoint.replace(e.f0.objID, e.f1);
                                    pointsIDMap.replace(e.f0.objID, e.f0);
                                }
                            }
                        }

                        // Sorting the pointDistFromQueryPoint map by value
                        List<Map.Entry<String, Double>> list = new LinkedList<>(pointDistFromQueryPoint.entrySet());
                        Collections.sort(list, Comparator.comparing(o -> o.getValue()));
                        for (Map.Entry<String, Double> map : list) {
                            sortedPointDistFromQueryPoint.put(map.getKey(), map.getValue());
                        }

                        // Logic to return the kNN (trajectory ID, distance) pairs
                        int counter = 0;
                        for (Map.Entry<String, Double> entry: sortedPointDistFromQueryPoint.entrySet()) {
                            if (counter == k) break; // to guarantee that only k outputs are generated
                            output.collect(Tuple2.of(pointsIDMap.get(entry.getKey()), entry.getValue()));
                            counter++;
                        }
                    }
                });
    }

    // WINDOW BASED Naive
    private DataStream<Tuple3<String, LineString, Double>> windowBasedNaive(DataStream<Point> pointStream, Point queryPoint, Integer k, int winSize, int winSlide, int allowedLateness) {

        // Spatial stream with Timestamps and Watermarks
        // Max Allowed Lateness: windowSize
        DataStream<Point> pointStreamWithTsAndWm =
                pointStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Point>(Time.seconds(allowedLateness)) {
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
        }).window(SlidingEventTimeWindows.of(Time.seconds(winSize), Time.seconds(winSlide)))
                .apply(new WindowFunction<Point, Tuple2<String, Double>, String, TimeWindow>() {

                    Map<String, Double> objMap = new HashMap<String, Double>();
                    HashMap<String, Double> sortedObjMap = new LinkedHashMap<>();

                    @Override
                    public void apply(String gridID, TimeWindow timeWindow, Iterable<Point> inputTuples, Collector<Tuple2<String, Double>> outputStream) throws Exception {

                        objMap.clear();
                        sortedObjMap.clear();

                        // compute the distance of all trajectory points w.r.t. query point and return the kNN (trajectory ID, distance) pairs
                        for (Point p : inputTuples) {

                            Double newDistance = DistanceFunctions.getPointPointEuclideanDistance(p.point.getX(), p.point.getY(), queryPoint.point.getX(), queryPoint.point.getY());
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
                }).window(SlidingEventTimeWindows.of(Time.seconds(winSize), Time.seconds(winSlide)))
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
        return windowedJoindKNN.windowAll(SlidingEventTimeWindows.of(Time.seconds(winSize), Time.seconds(winSlide)))
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
