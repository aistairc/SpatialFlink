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

package GeoFlink.spatialOperators.range;

import GeoFlink.spatialIndices.SpatialIndex;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.SpatialOperator;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

public abstract class RangeQuery<T extends SpatialObject, K extends SpatialObject> extends SpatialOperator implements Serializable {
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

    public void initializeRangeQuery(QueryConfiguration conf, SpatialIndex index){
        this.setQueryConfiguration(conf);
        this.setSpatialIndex(index);
    }

    public abstract DataStream<T> run(DataStream<T> stream, Set<K> obj, double queryRadius);

    protected class PolygonTrigger extends Trigger<Polygon, TimeWindow> {

        private int slideStep;
        ValueStateDescriptor<Boolean> firstWindowDesc = new ValueStateDescriptor<Boolean>("isFirstWindow", Boolean.class);

        //ctor
        public PolygonTrigger(){}
        public PolygonTrigger(int slideStep){
            this.slideStep = slideStep;
        }

        @Override
        public TriggerResult onElement(Polygon polygon, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

            ValueState<Boolean> firstWindow = triggerContext.getPartitionedState(firstWindowDesc);
            //Using states manage the first window, so that all the tuples can be processed
            if(firstWindow.value() == null){
                firstWindow.update(false);
                return TriggerResult.CONTINUE;
            }
            else {
                if (polygon.timeStampMillisec >= (timeWindow.getEnd() - (slideStep * 1000L)))
                    return TriggerResult.CONTINUE; // Do nothing
                else
                    return TriggerResult.PURGE; // Delete
            }
        }
        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }
        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }
        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        }
    }

    // Misc Classes
    protected class GetCellsFilteredByLayer extends RichFilterFunction<Tuple2<String, Integer>>
    {
        private final HashSet<String> CellIDs; // CellIDs are input parameters

        //ctor
        public GetCellsFilteredByLayer(HashSet<String> CellIDs)
        {
            this.CellIDs = CellIDs;
        }

        @Override
        public boolean filter(Tuple2<String, Integer> cellIDCount) throws Exception
        {
            return CellIDs.contains(cellIDCount.f0);
        }
    }

    protected class CellBasedLineStringFlatMap implements FlatMapFunction<LineString, LineString>{

        Set<String> neighboringCells = new HashSet<String>();

        //ctor
        public CellBasedLineStringFlatMap() {}
        public CellBasedLineStringFlatMap(Set<String> neighboringCells) {
            this.neighboringCells = neighboringCells;
        }

        @Override
        public void flatMap(LineString lineString, Collector<LineString> output) throws Exception {

            // If a polygon is either a CN or GN
            LineString outputLineString;
            for(String gridID: lineString.gridIDsSet) {
                if (neighboringCells.contains(gridID)) {
                    outputLineString = new LineString(lineString.objID, Arrays.asList(lineString.lineString.getCoordinates()), lineString.gridIDsSet, gridID, lineString.boundingBox);
                    output.collect(outputLineString);
                    return;
                }
            }
        }
    }
}