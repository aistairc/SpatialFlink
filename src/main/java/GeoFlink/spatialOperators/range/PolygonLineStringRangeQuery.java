package GeoFlink.spatialOperators.range;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.QueryType;
import GeoFlink.utils.DistanceFunctions;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PolygonLineStringRangeQuery extends RangeQuery<Polygon, LineString> {
    public PolygonLineStringRangeQuery(QueryConfiguration conf, SpatialIndex index) {
        super.initializeRangeQuery(conf, index);
    }

    public DataStream<Polygon> run(DataStream<Polygon> polygonStream, Set<LineString> queryLineStringSet, double queryRadius) {
        boolean approximateQuery = this.getQueryConfiguration().isApproximateQuery();
        int allowedLateness = this.getQueryConfiguration().getAllowedLateness();

        UniformGrid uGrid = (UniformGrid) this.getSpatialIndex();
        //--------------- Real-time - LINESTRING - POLYGON -----------------//
        if (this.getQueryConfiguration().getQueryType() == QueryType.RealTime) {
            //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((LineString[])queryLineStringSet.toArray())[0]);
            //Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, ((LineString[])queryLineStringSet.toArray())[0], guaranteedNeighboringCells);
            //Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(),candidateNeighboringCells.stream()).collect(Collectors.toSet());

            HashSet<String> candidateNeighboringCells = new HashSet<>();
            HashSet<String> guaranteedNeighboringCells = new HashSet<>();

            for(LineString lineString:queryLineStringSet) {
                //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((Polygon[]) queryPolygonSet.toArray())[0]);
                //Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, ((Polygon[]) queryPolygonSet.toArray())[0], guaranteedNeighboringCells);
                guaranteedNeighboringCells.addAll(uGrid.getGuaranteedNeighboringCells(queryRadius, lineString));
                candidateNeighboringCells.addAll(uGrid.getCandidateNeighboringCells(queryRadius, lineString, guaranteedNeighboringCells));
                //candidateNeighboringCells.addAll(guaranteedNeighboringCells);
            }
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(), candidateNeighboringCells.stream()).collect(Collectors.toSet());

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = polygonStream.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells)).startNewChain();

            return filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).flatMap(new FlatMapFunction<Polygon, Polygon>() {
                @Override
                public void flatMap(Polygon poly, Collector<Polygon> collector) throws Exception {

                    int cellIDCounter = 0;
                    for(String polyGridID: poly.gridIDsSet) {

                        if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                            cellIDCounter++;
                            // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                            if(cellIDCounter == poly.gridIDsSet.size()){
                                collector.collect(poly);
                            }
                        }
                        else { // candidate neighbors

                            double distance;
                            for(LineString ls:queryLineStringSet) {

                                if (approximateQuery) {
                                    distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(ls.boundingBox, poly.boundingBox);
                                } else {
                                    distance = DistanceFunctions.getDistance(ls, poly);
                                }

                                if (distance <= queryRadius) {
                                    collector.collect(poly);
                                    break;
                                }

                                /*
                                if (approximateQuery) {
                                    distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(((LineString[]) queryLineStringSet.toArray())[0].boundingBox, lineString.boundingBox);
                                } else {
                                    distance = DistanceFunctions.getDistance(((LineString[]) queryLineStringSet.toArray())[0], lineString);
                                }

                                if (distance <= queryRadius) {
                                    collector.collect(lineString);
                                }*/
                            }
                            break;
                        }
                    }

                }
            }).name("Real-time - LINESTRING - POLYGON");
        }
        //--------------- WINDOW-based - LINESTRING - POLYGON -----------------//
        else if (this.getQueryConfiguration().getQueryType() == QueryType.WindowBased) {
            int windowSize = this.getQueryConfiguration().getWindowSize();
            int slideStep = this.getQueryConfiguration().getSlideStep();

            //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((LineString[])queryLineStringSet.toArray())[0]);
            //Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, ((LineString[])queryLineStringSet.toArray())[0], guaranteedNeighboringCells);
            //Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(),candidateNeighboringCells.stream()).collect(Collectors.toSet());

            HashSet<String> candidateNeighboringCells = new HashSet<>();
            HashSet<String> guaranteedNeighboringCells = new HashSet<>();

            for(LineString lineString:queryLineStringSet) {
                //Set<String> guaranteedNeighboringCells = uGrid.getGuaranteedNeighboringCells(queryRadius, ((Polygon[]) queryPolygonSet.toArray())[0]);
                //Set<String> candidateNeighboringCells = uGrid.getCandidateNeighboringCells(queryRadius, ((Polygon[]) queryPolygonSet.toArray())[0], guaranteedNeighboringCells);
                guaranteedNeighboringCells.addAll(uGrid.getGuaranteedNeighboringCells(queryRadius, lineString));
                candidateNeighboringCells.addAll(uGrid.getCandidateNeighboringCells(queryRadius, lineString, guaranteedNeighboringCells));
                //candidateNeighboringCells.addAll(guaranteedNeighboringCells);
            }
            Set<String> neighboringCells = Stream.concat(guaranteedNeighboringCells.stream(), candidateNeighboringCells.stream()).collect(Collectors.toSet());

            DataStream<Polygon> streamWithTsAndWm =
                    polygonStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Polygon>(Time.seconds(allowedLateness)) {
                        @Override
                        public long extractTimestamp(Polygon p) {
                            return p.timeStampMillisec;
                        }
                    }).startNewChain();

            // Filtering out the polygons which lie greater than queryRadius of the query point
            DataStream<Polygon> filteredPolygons = streamWithTsAndWm.flatMap(new HelperClass.cellBasedPolygonFlatMap(neighboringCells));

            return filteredPolygons.keyBy(new KeySelector<Polygon, String>() {
                @Override
                public String getKey(Polygon poly) throws Exception {
                    return poly.gridID;
                }
            }).window(SlidingProcessingTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideStep)))
                    .apply(new WindowFunction<Polygon, Polygon, String, TimeWindow>() {
                        @Override
                        public void apply(String gridID, TimeWindow timeWindow, Iterable<Polygon> polygonIterator, Collector<Polygon> neighbors) throws Exception {

                            for (Polygon poly : polygonIterator) {
                                int cellIDCounter = 0;
                                for(String polyGridID: poly.gridIDsSet) {

                                    if (guaranteedNeighboringCells.contains(polyGridID)) { // guaranteed neighbors
                                        cellIDCounter++;
                                        // If all the polygon bbox cells are guaranteed neighbors (GNs) then the polygon is GN
                                        if(cellIDCounter == poly.gridIDsSet.size()){
                                            neighbors.collect(poly);
                                        }
                                    }
                                    else { // candidate neighbors

                                        double distance;

                                        for(LineString ls:queryLineStringSet) {

                                            if (approximateQuery) {
                                                distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(ls.boundingBox, poly.boundingBox);
                                            } else {
                                                distance = DistanceFunctions.getDistance(ls, poly);
                                            }

                                            if (distance <= queryRadius) {
                                                neighbors.collect(poly);
                                                break;
                                            }
                                        }

                                        /*
                                        if(approximateQuery) {
                                            distance = DistanceFunctions.getBBoxBBoxMinEuclideanDistance(poly.boundingBox, ((LineString[])queryLineStringSet.toArray())[0].boundingBox);
                                        }else{
                                            distance = DistanceFunctions.getDistance(poly, ((LineString[])queryLineStringSet.toArray())[0]);
                                        }

                                        if (distance <= queryRadius){
                                            neighbors.collect(poly);
                                        }*/
                                        break;
                                    }
                                }
                            }

                        }
                    }).name("Window-based - LINESTRING - POLYGON");
        } else {
            throw new IllegalArgumentException("Not yet support");
        }
    }
}
