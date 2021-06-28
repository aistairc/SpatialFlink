package GeoFlink.utils;

import java.util.Map;

public class ConfigType {
    private boolean clusterMode;
    private String kafkaBootStrapServers;
    private Map<String, Object> stream1;
    private Map<String, Object> stream2;
    private Map<String, Object> query;
    private Map<String, Object> window;

    public boolean isClusterMode() {
        return clusterMode;
    }

    public void setClusterMode(boolean clusterMode) {
        this.clusterMode = clusterMode;
    }

    public String getKafkaBootStrapServers() {
        return kafkaBootStrapServers;
    }

    public void setKafkaBootStrapServers(String kafkaBootStrapServers) {
        this.kafkaBootStrapServers = kafkaBootStrapServers;
    }

    public Map<String, Object> getStream1() {
        return stream1;
    }

    public void setStream1(Map<String, Object> stream1) {
        this.stream1 = stream1;
    }

    public Map<String, Object> getStream2() {
        return stream2;
    }

    public void setStream2(Map<String, Object> stream2) {
        this.stream2 = stream2;
    }

    public Map<String, Object> getQuery() {
        return query;
    }

    public void setQuery(Map<String, Object> query) {
        this.query = query;
    }

    public Map<String, Object> getWindow() {
        return window;
    }

    public void setWindow(Map<String, Object> window) {
        this.window = window;
    }

    @Override
    public String toString() {
        return "clusterMode=" + clusterMode + ", kafkaBootStrapServers=" + kafkaBootStrapServers + ", stream1=" + stream1 +
                ", stream2=" + stream2 + ", query=" + query + ", window=" + window;
    }
}
