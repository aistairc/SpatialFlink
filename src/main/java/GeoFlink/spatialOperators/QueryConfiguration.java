package GeoFlink.spatialOperators;

import java.io.Serializable;

public class QueryConfiguration implements Serializable {
    private QueryType queryType;
    private int windowSize = 0;
    private int slideStep = 0;
    private int allowedLateness = 0;
    private boolean approximateQuery = false;

    public QueryConfiguration() {}

    public QueryConfiguration(QueryType queryType) {
        this.queryType = queryType;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    public int getSlideStep() {
        return slideStep;
    }

    public void setSlideStep(int slideStep) {
        this.slideStep = slideStep;
    }

    public int getAllowedLateness() {
        return allowedLateness;
    }

    public void setAllowedLateness(int allowedLateness) {
        this.allowedLateness = allowedLateness;
    }

    public boolean isApproximateQuery() {
        return approximateQuery;
    }

    public void setApproximateQuery(boolean approximateQuery) {
        this.approximateQuery = approximateQuery;
    }
}