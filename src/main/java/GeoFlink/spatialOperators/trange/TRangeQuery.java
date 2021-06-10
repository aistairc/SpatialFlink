package GeoFlink.spatialOperators.trange;

import GeoFlink.spatialObjects.SpatialObject;
import GeoFlink.spatialOperators.QueryConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.Set;

public abstract class TRangeQuery<T extends SpatialObject, S extends SpatialObject, K extends SpatialObject> implements Serializable {
    private QueryConfiguration queryConfiguration;

    public QueryConfiguration getQueryConfiguration() {
        return queryConfiguration;
    }

    public void setQueryConfiguration(QueryConfiguration queryConfiguration) {
        this.queryConfiguration = queryConfiguration;
    }

    public void initializeTRangeQuery(QueryConfiguration conf){
        this.setQueryConfiguration(conf);
    }

    public abstract DataStream<T> run(DataStream<S> ordinaryStream, Set<K> objSet);
}
