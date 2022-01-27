package GeoFlink.utils;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.locationtech.jts.geom.Coordinate;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Params {
    /**
     * Config File
     */
    public final String YAML_CONFIG = "geoflink-conf.yml";
    public final String YAML_PATH = new File(".").getAbsoluteFile().getParent().toString() + File.separator +
                                    "conf" +  File.separator + YAML_CONFIG;

    /**
     * Parameters
     */
    /* Cluster */
    public boolean clusterMode = false;
    public int parallelism = 1;
    public String kafkaBootStrapServers;

    /* Stream1 Input */
    public String inputTopicName1;
    public String inputFormat1;
    public String dateFormatStr1;
    public List<String> geoJSONSchemaAttr1 = new ArrayList<>();
    public List<Integer> csvTsvSchemaAttr1 = new ArrayList<>();
    public List<Double> gridBBox1 = new ArrayList<>();
    public int numGridCells1 = 0;
    public int cellLength1 = 0;
    public String inputDelimiter1;
    public String charset1;

    /* Stream2 Input */
    public String inputTopicName2;
    public String inputFormat2;
    public String dateFormatStr2;
    public List<String> geoJSONSchemaAttr2 = new ArrayList<>();
    public List<Integer> csvTsvSchemaAttr2 = new ArrayList<>();
    public List<Double> gridBBox2 = new ArrayList<>();
    public int numGridCells2 = 0;
    public int cellLength2 = Integer.MIN_VALUE;
    public String inputDelimiter2;
    public String charset2;

    /* Output Stream */
    public String outputTopicName;
    public String outputDelimiter;

    /* Query */
    public int queryOption = Integer.MIN_VALUE;
    public boolean queryApproximate = false;
    public double queryRadius = Double.MIN_VALUE; // Default 10x10 Grid
    public String queryAggregateFunction;  // "ALL", "SUM", "AVG", "MIN", "MAX" (Default = ALL)
    public int queryK = Integer.MIN_VALUE; // k denotes filter size in filter query
    public int queryOmegaDuration = Integer.MIN_VALUE;
    public Set<String> queryTrajIDSet;
    public List<Coordinate> queryPoints = new ArrayList<>();
    public List<List<Coordinate>> queryPolygons = new ArrayList<>();
    public List<List<Coordinate>> queryLineStrings = new ArrayList<>();
    public long queryTrajDeletion = Long.MIN_VALUE;
    public int queryOutOfOrderTuples = Integer.MIN_VALUE;

    /* Window */
    public String windowType;
    public int windowInterval = Integer.MIN_VALUE;
    public int windowStep = Integer.MIN_VALUE;


    public Params() throws NullPointerException, IllegalArgumentException, NumberFormatException {
        ConfigType config = getYamlConfig(YAML_PATH);

        /* Cluster */
        clusterMode = config.isClusterMode();
        if ((kafkaBootStrapServers = config.getKafkaBootStrapServers()) == null) {
            throw new NullPointerException("kafkaBootStrapServers is " + config.getKafkaBootStrapServers());
        }
        /* Input Stream1  */
        try {
            if ((inputTopicName1 = (String)config.getInputStream1().get("topicName")) == null) {
                throw new NullPointerException("inputTopicName1 is " + config.getInputStream1().get("topicName"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("inputTopicName1 : " + e);
        }
        try {
            if ((inputFormat1 = (String)config.getInputStream1().get("format")) == null) {
                throw new NullPointerException("format1 is " + config.getInputStream1().get("format"));
            }
            else {
                List<String> validParam = Arrays.asList("GeoJSON", "WKT", "CSV", "TSV");
                if (!validParam.contains(inputFormat1)) {
                    throw new IllegalArgumentException(
                            "format1 is " + inputFormat1 + ". " +
                                    "Valid value is \"GeoJSON\", \"WKT\", \"CSV\" or \"TSV\".");
                }
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("format1 : " + e);
        }
        try {
            if ((dateFormatStr1 = (String)config.getInputStream1().get("dateFormat")) == null) {
                throw new NullPointerException("dateFormat1 is " + config.getInputStream1().get("dateFormat"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("dateFormat1 : " + e);
        }
        try {
            if ((geoJSONSchemaAttr1 = (ArrayList)config.getInputStream1().get("geoJSONSchemaAttr")) == null) {
                throw new NullPointerException("geoJSONSchemaAttr1 is " + config.getInputStream1().get("geoJSONSchemaAttr"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("geoJSONSchemaAttr1 : " + e);
        }
        try {
            if ((csvTsvSchemaAttr1 = (ArrayList)config.getInputStream1().get("csvTsvSchemaAttr")) == null) {
                throw new NullPointerException("csvTsvSchemaAttr1 is " + config.getInputStream1().get("csvTsvSchemaAttr"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("csvTsvSchemaAttr1 : " + e);
        }
        try {
            if ((gridBBox1 = (ArrayList)config.getInputStream1().get("gridBBox")) == null) {
                throw new NullPointerException("gridBBox1 is " + config.getInputStream1().get("gridBBox"));
            }
            if (gridBBox1.size() != 4) {
                throw new IllegalArgumentException("gridBBox1 num is " + gridBBox1.size());
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("gridBBox1 : " + e);
        }
        try {
            if(config.getInputStream1().get("numGridCells") == null) {
                throw new NullPointerException("numGridCells1 is " + config.getInputStream1().get("numGridCells"));
            }
            else {
                numGridCells1 = (int)config.getInputStream1().get("numGridCells");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("numGridCells1 : " + e);
        }
        try {
            if(config.getInputStream1().get("cellLength") == null) {
                throw new NullPointerException("cellLength1 is " + config.getInputStream1().get("cellLength"));
            }
            else {
                cellLength1 = (int)config.getInputStream1().get("cellLength");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("cellLength1 : " + e);
        }
        try {
            if((inputDelimiter1 = (String)config.getInputStream1().get("delimiter")) == null) {
                throw new NullPointerException("inputDelimiter1 is " + config.getQuery().get("delimiter"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("inputDelimiter1 : " + e);
        }
        try {
            if((charset1 = (String)config.getInputStream1().get("charset")) == null) {
                throw new NullPointerException("charset1 is " + config.getInputStream1().get("charset"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("charset1 : " + e);
        }

        /* Input Stream2 */
        try {
            if ((inputTopicName2 = (String)config.getInputStream2().get("topicName")) == null) {
                throw new NullPointerException("inputTopicName2 is " + config.getInputStream2().get("topicName"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("inputTopicName2 : " + e);
        }
        try {
            if ((inputFormat2 = (String)config.getInputStream2().get("format")) == null) {
                throw new NullPointerException("format2 is " + config.getInputStream2().get("format"));
            }
            else {
                List<String> validParam = Arrays.asList("GeoJSON", "WKT", "CSV", "TSV");
                if (!validParam.contains(inputFormat2)) {
                    throw new IllegalArgumentException(
                            "format2 is " + inputFormat2 + ". " +
                                    "Valid value is \"GeoJSON\", \"WKT\", \"CSV\" or \"TSV\".");
                }
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("format2 : " + e);
        }
        try {
            if ((dateFormatStr2 = (String)config.getInputStream2().get("dateFormat")) == null) {
                throw new NullPointerException("dateFormat2 is " + config.getInputStream2().get("dateFormat"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("dateFormat2 : " + e);
        }
        try {
            if ((geoJSONSchemaAttr2 = (ArrayList)config.getInputStream2().get("geoJSONSchemaAttr")) == null) {
                throw new NullPointerException("geoJSONSchemaAttr2 is " + config.getInputStream2().get("geoJSONSchemaAttr"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("geoJSONSchemaAttr2 : " + e);
        }
        try {
            if ((csvTsvSchemaAttr2 = (ArrayList)config.getInputStream2().get("csvTsvSchemaAttr")) == null) {
                throw new NullPointerException("csvTsvSchemaAttr2 is " + config.getInputStream2().get("csvTsvSchemaAttr"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("csvTsvSchemaAttr2 : " + e);
        }
        try {
            if ((gridBBox2 = (ArrayList)config.getInputStream2().get("gridBBox")) == null) {
                throw new NullPointerException("gridBBox2 is " + config.getInputStream2().get("gridBBox"));
            }
            if (gridBBox2.size() != 4) {
                throw new IllegalArgumentException("gridBBox2 num is " + gridBBox2.size());
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("gridBBox2 : " + e);
        }
        try {
            if(config.getInputStream2().get("numGridCells") == null) {
                throw new NullPointerException("numGridCells2 is " + config.getInputStream2().get("numGridCells"));
            }
            else {
                numGridCells2 = (int)config.getInputStream2().get("numGridCells");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("numGridCells2 : " + e);
        }
        try {
            if(config.getInputStream2().get("cellLength") == null) {
                throw new NullPointerException("cellLength2 is " + config.getInputStream2().get("cellLength"));
            }
            else {
                cellLength2 = (int)config.getInputStream2().get("cellLength");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("cellLength2 : " + e);
        }
        try {
            if((inputDelimiter2 = (String)config.getInputStream2().get("delimiter")) == null) {
                throw new NullPointerException("inputDelimiter2 is " + config.getInputStream2().get("delimiter"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("inputDelimiter2 : " + e);
        }
        try {
            if((charset2 = (String)config.getInputStream2().get("charset")) == null) {
                throw new NullPointerException("charset2 is " + config.getQuery().get("charset"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("charset2 : " + e);
        }

        /* Output Stream */
        try {
            if ((outputTopicName = (String)config.getOutputStream().get("topicName")) == null) {
                throw new NullPointerException("outputTopicName is " + config.getOutputStream().get("topicName"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("outputTopicName : " + e);
        }
        try {
            if((outputDelimiter = (String)config.getOutputStream().get("delimiter")) == null) {
                throw new NullPointerException("outputDelimiter is " + config.getOutputStream().get("delimiter"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("outputDelimiter : " + e);
        }

        /* Query */
        try {
            if(config.getQuery().get("option") == null) {
                throw new NullPointerException("option is " + config.getQuery().get("option"));
            }
            else {
                queryOption = (int)config.getQuery().get("option");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("option : " + e);
        }
        try {
            if(config.getQuery().get("parallelism") == null) {
                throw new NullPointerException("parallelism is " + config.getQuery().get("parallelism"));
            }
            else {
                parallelism = (int)config.getQuery().get("parallelism");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("parallelism : " + e);
        }

        try {
            if(config.getQuery().get("approximate") == null) {
                throw new NullPointerException("approximate is " + config.getQuery().get("approximate"));
            }
            else {
                queryApproximate = (boolean)config.getQuery().get("approximate");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("approximate : " + e);
        }
        try {
            if(config.getQuery().get("radius") == null) {
                throw new NullPointerException("radius is " + config.getQuery().get("radius"));
            }
            else {
                queryRadius = (double)config.getQuery().get("radius");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("radius : " + e);
        }
        try {
            if ((queryAggregateFunction = (String)config.getQuery().get("aggregateFunction")) == null) {
                throw new NullPointerException("aggregateFunction is " + config.getQuery().get("aggregateFunction"));
            }
            else {
                List<String> validParam = Arrays.asList("ALL", "SUM", "AVG", "MIN", "MAX");
                if (!validParam.contains(queryAggregateFunction)) {
                    throw new IllegalArgumentException(
                            "aggregateFunction is " + queryAggregateFunction + ". " +
                                    "Valid value is \"ALL\", \"SUM\", \"AVG\", \"MIN\" or \"MAX\".");
                }
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("aggregateFunction : " + e);
        }
        try {
            if(config.getQuery().get("k") == null) {
                throw new NullPointerException("k is " + config.getQuery().get("k"));
            }
            else {
                queryK = (int)config.getQuery().get("k");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("k : " + e);
        }
        try {
            if(config.getQuery().get("omegaDuration") == null) {
                throw new NullPointerException("omegaDuration is " + config.getQuery().get("omegaDuration"));
            }
            else {
                queryOmegaDuration = (int)config.getQuery().get("omegaDuration");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("omegaDuration : " + e);
        }
        try {
            List<Integer> trajIDs;
            if((trajIDs = (ArrayList)config.getQuery().get("trajIDs")) == null) {
                throw new NullPointerException("trajIDs is " + config.getQuery().get("trajIDs"));
            }
            else {
                List<String> arrTrajID = new ArrayList<>();
                for (int trajID : trajIDs) {
                    arrTrajID.add(Integer.valueOf(trajID).toString());
                }
                String[] strTrajIDs = arrTrajID.toArray(new String[0]);
                queryTrajIDSet = Stream.of(strTrajIDs).collect(Collectors.toSet());
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException("trajIDs : " + e);
        }
        try {
            List<List<Double>> coordinates;
            if((coordinates = (List<List<Double>>)config.getQuery().get("queryPoints")) == null) {
                throw new NullPointerException("queryPoints is " + config.getQuery().get("queryPoints"));
            }
            for (List<Double> c : coordinates) {
                queryPoints.add(new Coordinate(c.get(0), c.get(1)));
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException("queryPoints : " + e);
        }
        try {
            List<List<List<Double>>> listCoordinates;
            if((listCoordinates = (List<List<List<Double>>>)config.getQuery().get("queryPolygons")) == null) {
                throw new NullPointerException("queryPolygons is " + config.getQuery().get("queryPolygons"));
            }
            List<Coordinate> list;
            for (List<List<Double>> coordinates : listCoordinates) {
                list = new ArrayList<>();
                for (List<Double> c : coordinates) {
                    list.add(new Coordinate(c.get(0), c.get(1)));
                }
                queryPolygons.add(list);
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("queryPolygons : " + e);
        }
        try {
            List<List<List<Double>>> listCoordinates;
            if((listCoordinates = (List<List<List<Double>>>)config.getQuery().get("queryLineStrings")) == null) {
                throw new NullPointerException("queryLineStrings is " + config.getQuery().get("queryLineStrings"));
            }
            List<Coordinate> list;
            for (List<List<Double>> coordinates : listCoordinates) {
                list = new ArrayList<>();
                for (List<Double> c : coordinates) {
                    list.add(new Coordinate(c.get(0), c.get(1)));
                }
                queryLineStrings.add(list);
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("queryLineStrings : " + e);
        }
        try {
            Map<String, Object> thresholds;
            if((thresholds = (Map<String, Object>)config.getQuery().get("thresholds")) == null) {
                throw new NullPointerException("thresholds is " + config.getQuery().get("thresholds"));
            }
            else {
                if (thresholds.get("trajDeletion") == null) {
                    throw new NullPointerException("trajDeletion is " + thresholds.get("trajDeletion"));
                }
                else {
                    queryTrajDeletion = ((Integer)thresholds.get("trajDeletion")).longValue();
                }
                if (thresholds.get("outOfOrderTuples") == null) {
                    throw new NullPointerException("outOfOrderTuples is " + thresholds.get("outOfOrderTuples"));
                }
                else {
                    queryOutOfOrderTuples = (int)thresholds.get("outOfOrderTuples");
                }
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("thresholds : " + e);
        }

        /* Window */
        try {
            if((windowType = (String)config.getWindow().get("type")) == null) {
                throw new NullPointerException("windowType is " + config.getWindow().get("type"));
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("windowType : " + e);
        }
        try {
            if(config.getWindow().get("interval") == null) {
                throw new NullPointerException("interval is " + config.getWindow().get("interval"));
            }
            else {
                windowInterval = (int)config.getWindow().get("interval");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("interval : " + e);
        }
        try {
            if(config.getWindow().get("step") == null) {
                throw new NullPointerException("step is " + config.getWindow().get("step"));
            }
            else {
                windowStep = (int)config.getWindow().get("step");
            }
        }
        catch (ClassCastException e) {
            throw new IllegalArgumentException("step : " + e);
        }
    }

    private ConfigType getYamlConfig(String path) {
        File file = new File(path);
        Yaml yaml = new Yaml();
        FileInputStream input;
        InputStreamReader stream;
        try {
            input = new FileInputStream(file);
            stream = new InputStreamReader(input, "UTF-8");
            return (ConfigType)yaml.load(stream);
        }
        catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
        catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public String toString() {
        return "clusterMode = " + clusterMode + ", " +
                "kafkaBootStrapServers = " + kafkaBootStrapServers + ", " +
                "\n" +
                "inputTopicName1 = " + inputTopicName1 + ", " +
                "format1 = " + inputFormat1 + ", " +
                "dateFormatStr1 = " + dateFormatStr1 + ", " +
                "geoJSONSchemaAttr1 = " + geoJSONSchemaAttr1 + ", " +
                "csvTsvSchemaAttr1 = " + csvTsvSchemaAttr1 + ", " +
                "gridBBox1 = " + gridBBox1 + ", " +
                "numGridCells1 = " + numGridCells1 + ", " +
                "cellLength1 = " + cellLength1 + ", " +
                "inputDelimiter1 = " + inputDelimiter1 + ", " +
                "charset1 = " + charset1 + ", " +
                "\n" +
                "inputTopicName2 = " + inputTopicName2 + ", " +
                "format2 = " + inputFormat2 + ", " +
                "dateFormatStr2 = " + dateFormatStr2 + ", " +
                "geoJSONSchemaAttr2 = " + geoJSONSchemaAttr2 + ", " +
                "csvTsvSchemaAttr2 = " + csvTsvSchemaAttr2 + ", " +
                "gridBBox2 = " + gridBBox2 + ", " +
                "numGridCells2 = " + numGridCells2 + ", " +
                "cellLength2 = " + cellLength2 + ", " +
                "inputDelimiter2 = " + inputDelimiter2 + ", " +
                "charset2 = " + charset2 + ", " +
                "\n" +
                "outputTopicName = " + outputTopicName + ", " +
                "outputDelimiter = " + outputDelimiter + ", " +
                "\n" +
                "queryOption = " + queryOption + ", " +
                "queryApproximate = " + queryApproximate + ", " +
                "queryRadius = " + queryRadius + ", " +
                "queryAggregateFunction = " + queryAggregateFunction + ", " +
                "queryK = " + queryK + ", " +
                "queryOmegaDuration = " + queryOmegaDuration + ", " +
                "queryTrajIDSet = " + queryTrajIDSet + ", " +
                "queryPoints = " + queryPoints + ", " +
                "queryPolygons = " + queryPolygons + ", " +
                "queryLineStrings = " + queryLineStrings + ", " +
                "queryTrajDeletion = " + queryTrajDeletion + ", " +
                "queryOutOfOrderTuples = " + queryOutOfOrderTuples + ", " +
                "\n" +
                "windowType = " + windowType + ", " +
                "windowInterval = " + windowInterval + ", " +
                "windowStep = " + windowStep;
    }
}
