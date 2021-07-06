package GeoFlink.utils;

import java.util.Map;

public class ConfigType {
    private boolean clusterMode;
    private String kafkaBootStrapServers;
    private Map<String, Object> stream1Input;
    private Map<String, Object> stream2Input;
    private Map<String, Object> stream1Output;
    private Map<String, Object> stream2Output;
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

    public Map<String, Object> getStream1Input() {
        return stream1Input;
    }

    public void setStream1Input(Map<String, Object> stream1Input) {
        this.stream1Input = stream1Input;
    }

    public Map<String, Object> getStream2Input() {
        return stream2Input;
    }

    public void setStream2Input(Map<String, Object> stream2Input) {
        this.stream2Input = stream2Input;
    }

    public Map<String, Object> getStream1Output() {
        return stream1Output;
    }

    public void setStream1Output(Map<String, Object> stream1Output) {
        this.stream1Output = stream1Output;
    }

    public Map<String, Object> getStream2Output() {
        return stream2Output;
    }

    public void setStream2Output(Map<String, Object> stream2Output) {
        this.stream2Output = stream2Output;
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
        return "clusterMode=" + clusterMode + ", kafkaBootStrapServers=" + kafkaBootStrapServers + ", stream1Input=" + stream1Input +
                ", stream2Input=" + stream2Input + ", stream1Output=" + stream1Output + ", stream2Output=" + stream2Output +
                ", query=" + query + ", window=" + window;
    }
}
