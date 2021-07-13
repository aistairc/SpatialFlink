package GeoFlink.utils;

import java.util.Map;

public class ConfigType {
    private boolean clusterMode;
    private String kafkaBootStrapServers;
    private Map<String, Object> inputStream1;
    private Map<String, Object> inputStream2;
    private Map<String, Object> outputStream;
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

    public Map<String, Object> getInputStream1() {
        return inputStream1;
    }

    public void setInputStream1(Map<String, Object> inputStream1) {
        this.inputStream1 = inputStream1;
    }

    public Map<String, Object> getInputStream2() {
        return inputStream2;
    }

    public void setInputStream2(Map<String, Object> inputStream2) {
        this.inputStream2 = inputStream2;
    }

    public Map<String, Object> getOutputStream() {
        return outputStream;
    }

    public void setOutputStream(Map<String, Object> outputStream) {
        this.outputStream = outputStream;
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
        return "clusterMode=" + clusterMode + ", kafkaBootStrapServers=" + kafkaBootStrapServers + ", inputStream1=" + inputStream1 +
                ", inputStream2=" + inputStream2 + ", outputStream=" + outputStream +
                ", query=" + query + ", window=" + window;
    }
}
