package GeoFlink.spatialOperators.tFilter;

import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.spatialOperators.QueryConfiguration;
import GeoFlink.spatialOperators.SpatialOperator;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.Set;

public abstract class TFilterQuery<T extends SpatialObject> extends SpatialOperator implements Serializable {
    private QueryConfiguration queryConfiguration;

    public QueryConfiguration getQueryConfiguration() {
        return queryConfiguration;
    }

    public void setQueryConfiguration(QueryConfiguration queryConfiguration) {
        this.queryConfiguration = queryConfiguration;
    }

    public void initializeKNNQuery(QueryConfiguration conf){
        this.setQueryConfiguration(conf);
    }

    public abstract DataStream<? extends SpatialObject> run(DataStream<T> stream, Set<String> trajIDSet);
}
