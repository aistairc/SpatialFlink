package GeoFlink.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Params {
    public int queryOption = Integer.MIN_VALUE;
    public String inputTopicName;
    public String queryTopicName;
    public String outputTopicName;
    public String inputFormat;
    public String dateFormatStr;
    public String queryDateFormatStr;
    public String aggregateFunction;  // "ALL", "SUM", "AVG", "MIN", "MAX" (Default = ALL)
    public double radius = Double.MIN_VALUE; // Default 10x10 Grid
    public int uniformGridSize = Integer.MIN_VALUE;
    public double cellLengthMeters = Double.MIN_VALUE;
    public int windowSize = Integer.MIN_VALUE;
    public int windowSlideStep = Integer.MIN_VALUE;
    public String windowType;
    public int k = Integer.MIN_VALUE; // k denotes filter size in filter query
    public boolean onCluster = false;
    public String bootStrapServers;
    public boolean approximateQuery = false;
    public Long inactiveTrajDeletionThreshold = Long.MIN_VALUE;
    public int allowedLateness = Integer.MIN_VALUE;
    public int omegaJoinDurationSeconds = Integer.MIN_VALUE;
    public double gridMinX = Double.MIN_VALUE;
    public double gridMaxX = Double.MIN_VALUE;
    public double gridMinY = Double.MIN_VALUE;
    public double gridMaxY = Double.MIN_VALUE;
    public double qGridMinX = Double.MIN_VALUE;
    public double qGridMaxX = Double.MIN_VALUE;
    public double qGridMinY = Double.MIN_VALUE;
    public double qGridMaxY = Double.MIN_VALUE;
    public String trajIDSet;
    public List<Coordinate> queryPointCoordinates = new ArrayList<Coordinate>();
    public List<Coordinate> queryLineStringCoordinates = new ArrayList<Coordinate>();
    public List<Coordinate> queryPolygonCoordinates = new ArrayList<Coordinate>();
    public List<String> ordinaryStreamAttributeNames = new ArrayList<String>(); // default order of attributes: objectID, timestamp
    public List<String> queryStreamAttributeNames = new ArrayList<String>();
    public List<Coordinate> queryPointSetCoordinates = new ArrayList<Coordinate>();

    public Params(ParameterTool parameters) throws NullPointerException, IllegalArgumentException, NumberFormatException {
        try {
            queryOption = Integer.parseInt(parameters.get("queryOption"));
        }
        catch (NullPointerException e) {
            System.out.println("queryOption is " + parameters.get("queryOption"));
        }
        if ((inputTopicName = parameters.get("inputTopicName")) == null) {
            throw new NullPointerException("inputTopicName is " + parameters.get("inputTopicName"));
        }
        if ((queryTopicName = parameters.get("queryTopicName")) == null) {
            throw new NullPointerException("queryTopicName is " +  parameters.get("queryTopicName"));
        }
        if ((inputFormat = parameters.get("inputFormat")) == null) {
            throw new NullPointerException("inputFormat is " + parameters.get("inputFormat"));
        }
        else {
            List<String> validParam = Arrays.asList("GeoJSON", "CSV", "TSV");
            if (!validParam.contains(inputFormat)) {
                throw new IllegalArgumentException(
                        "inputTopicName is " + inputFormat + ". " +
                                "Valid value is \"GeoJSON\", \"CSV\" or \"TSV\".");
            }
        }
        if ((dateFormatStr = parameters.get("dateFormat")) == null) {
            throw new NullPointerException("dateFormat is " + parameters.get("dateFormat"));
        }
        if ((queryDateFormatStr = parameters.get("queryDateFormat")) == null) {
            throw new NullPointerException("queryDateFormat is " + parameters.get("queryDateFormat"));
        }
        if ((aggregateFunction = parameters.get("aggregate")) == null) {
            throw new NullPointerException("aggregate is " + parameters.get("aggregate"));
        }
        else {
            List<String> validParam = Arrays.asList("ALL", "SUM", "AVG", "MIN", "MAX");
            if (!validParam.contains(aggregateFunction)) {
                throw new IllegalArgumentException(
                        "aggregateFunction is " + aggregateFunction + ". " +
                                "Valid value is \"ALL\", \"SUM\", \"AVG\", \"MIN\" or \"MAX\".");
            }
        }
        try {
            radius = Double.parseDouble(parameters.get("radius")); // Default 10x10 Grid
        }
        catch (NullPointerException e) {
            System.out.println("radius is " + parameters.get("radius"));
        }
        try {
            uniformGridSize = Integer.parseInt(parameters.get("uniformGridSize"));
        }
        catch (NullPointerException e) {
            System.out.println("uniformGridSize is " + parameters.get("uniformGridSize"));
        }
        try {
            cellLengthMeters = Double.parseDouble(parameters.get("cellLengthMeters"));
        }
        catch (NullPointerException e) {
            System.out.println("cellLengthMeters is " + parameters.get("cellLengthMeters"));
        }
        try {
            windowSize = Integer.parseInt(parameters.get("wInterval"));
        }
        catch (NullPointerException e) {
            System.out.println("wInterval is " + parameters.get("wInterval"));
        }
        try {
            windowSlideStep = Integer.parseInt(parameters.get("wStep"));
        }
        catch (NullPointerException e) {
            System.out.println("wStep is " + parameters.get("wStep"));
        }
        if ((windowType = parameters.get("wType")) == null) {
            throw new NullPointerException("wType is " + parameters.get("wType"));
        }
        try {
            k = Integer.parseInt(parameters.get("k")); // k denotes filter size in filter query
        }
        catch (NullPointerException e) {
            System.out.println("k is " + parameters.get("k"));
        }
        try {
            onCluster = Boolean.parseBoolean(parameters.get("onCluster"));
        }
        catch (NullPointerException e) {
            System.out.println("onCluster is " + parameters.get("onCluster"));
        }
        if ((bootStrapServers = parameters.get("kafkaBootStrapServers")) == null) {
            throw new NullPointerException("kafkaBootStrapServers is " + parameters.get("kafkaBootStrapServers"));
        }
        try {
            approximateQuery = Boolean.parseBoolean(parameters.get("approximateQuery"));
        }
        catch (NullPointerException e) {
            System.out.println("approximateQuery is " + parameters.get("approximateQuery"));
        }
        try {
            inactiveTrajDeletionThreshold = Long.parseLong(parameters.get("trajDeletionThreshold"));
        }
        catch (NullPointerException e) {
            System.out.println("trajDeletionThreshold is " + parameters.get("trajDeletionThreshold"));
        }
        try {
            allowedLateness = Integer.parseInt(parameters.get("outOfOrderAllowedLateness"));
        }
        catch (NullPointerException e) {
            System.out.println("outOfOrderAllowedLateness is " + parameters.get("outOfOrderAllowedLateness"));
        }
        try {
            omegaJoinDurationSeconds = Integer.parseInt(parameters.get("omegaJoinDuration"));
        }
        catch (NullPointerException e) {
            System.out.println("omegaJoinDuration is " + parameters.get("omegaJoinDuration"));
        }
        String gridBBox1;
        if ((gridBBox1 = parameters.get("gridBBox1")) == null) {
            throw new NullPointerException("gridBBox1 is " + parameters.get("gridBBox1"));
        }
        else {
            gridBBox1 = gridBBox1.replaceAll("\\[", "").replaceAll("]", "");
            String[] arrayGrid = gridBBox1.split(",");
            if (arrayGrid.length != 4) {
                throw new IllegalArgumentException(" Illegal gridBBox1 number of elements : " + gridBBox1);
            }
            gridMinX = Double.parseDouble(arrayGrid[0].trim());
            gridMinY = Double.parseDouble(arrayGrid[1].trim());
            gridMaxX = Double.parseDouble(arrayGrid[2].trim());
            gridMaxY = Double.parseDouble(arrayGrid[3].trim());
        }
        String gridBBox2;
        if ((gridBBox2 = parameters.get("gridBBox2")) == null) {
            throw new NullPointerException("gridBBox2 is " + parameters.get("gridBBox2"));
        }
        else {
            gridBBox2 = gridBBox2.replaceAll("\\[", "").replaceAll("]", "");
            String[] arrayGrid = gridBBox2.split(",");
            if (arrayGrid.length != 4) {
                throw new IllegalArgumentException(" Illegal gridBBox2 number of elements : " + gridBBox2);
            }
            qGridMinX = Double.parseDouble(arrayGrid[0].trim());
            qGridMinY = Double.parseDouble(arrayGrid[1].trim());
            qGridMaxX = Double.parseDouble(arrayGrid[2].trim());
            qGridMaxY = Double.parseDouble(arrayGrid[3].trim());
        }
        System.out.println(gridBBox2);
        System.out.println(qGridMinX + ", " + qGridMinY + ", " + qGridMaxX + ", " + qGridMaxY);
        if ((trajIDSet = parameters.get("trajIDSet")) == null) {
            throw new NullPointerException("trajIDSet is " + parameters.get("trajIDSet"));
        }
        try {
            queryPointCoordinates = HelperClass.getCoordinates(parameters.get("queryPoint"));
        }
        catch (NullPointerException e) {
            System.out.println("queryPoint is " + parameters.get("queryPoint"));
        }
        try {
            queryLineStringCoordinates = HelperClass.getCoordinates(parameters.get("queryLineString"));
        }
        catch (NullPointerException e) {
            System.out.println("queryLineString is " + parameters.get("queryLineString"));
        }
        try {
            queryPolygonCoordinates = HelperClass.getCoordinates(parameters.get("queryPolygon"));
        }
        catch (NullPointerException e) {
            System.out.println("queryPolygon is " + parameters.get("queryPolygon"));
        }
        try {
            ordinaryStreamAttributeNames = HelperClass.getParametersArray(parameters.get("ordinaryStreamAttributes")); // default order of attributes: objectID, timestamp
        }
        catch (NullPointerException e) {
            System.out.println("ordinaryStreamAttributes is " + parameters.get("ordinaryStreamAttributes"));
        }
        try {
            queryStreamAttributeNames = HelperClass.getParametersArray(parameters.get("queryStreamAttributes"));
        }
        catch (NullPointerException e) {
            System.out.println("queryStreamAttributes is " + parameters.get("queryStreamAttributes"));
        }
        try {
            queryPointSetCoordinates = HelperClass.getCoordinates(parameters.get("queryPointSet"));
        }
        catch (NullPointerException e) {
            System.out.println("queryLineString is " + parameters.get("queryLineString"));
        }
    }
}
