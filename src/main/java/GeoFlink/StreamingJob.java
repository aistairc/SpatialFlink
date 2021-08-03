/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package GeoFlink;

import GeoFlink.apps.StayTime;
import GeoFlink.apps.CheckIn;
import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.*;
import GeoFlink.spatialOperators.*;
import GeoFlink.spatialOperators.tJoin.TJoinQuery;
import GeoFlink.spatialOperators.tRange.TRangeQuery;
import GeoFlink.spatialOperators.tStats.PointTStatsQuery;
import GeoFlink.spatialOperators.join.*;
import GeoFlink.spatialOperators.knn.*;
import GeoFlink.spatialOperators.range.*;
import GeoFlink.spatialOperators.tAggregate.PointTAggregateQuery;
import GeoFlink.spatialOperators.tFilter.PointTFilterQuery;
import GeoFlink.spatialOperators.tJoin.PointPointTJoinQuery;
import GeoFlink.spatialOperators.tKnn.PointPointTKNNQuery;
import GeoFlink.spatialOperators.tRange.PointPolygonTRangeQuery;
import GeoFlink.spatialStreams.*;
import GeoFlink.utils.HelperClass;
import GeoFlink.utils.Params;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.locationtech.jts.geom.Coordinate;
import scala.Serializable;

import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamingJob implements Serializable {

	public static void main(String[] args) throws Exception {

		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		config.setString(RestOptions.BIND_PORT, "8082");

		// Set up the streaming execution environment
		final StreamExecutionEnvironment env;

		//--onCluster "false" --approximateQuery "false" --queryOption "5" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --queryDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
		//--onCluster "false" --approximateQuery "false" --queryOption "1" --inputTopicName "TaxiDrive17MillionGeoJSON" --queryTopicName "queryPointTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd HH:mm:ss" --queryDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "5" --wStep "3" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "115.50000" --gridMaxX "117.60000" --gridMinY "39.60000" --gridMaxY "41.10000" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[116.14319183444924, 40.07271444145411]" --queryPolygon "[116.14319183444924, 40.07271444145411], [116.14305232274667, 40.06231150684208], [116.16313670438304, 40.06152322130762], [116.14319183444924, 40.07271444145411]" --queryLineString "[116.14319183444924, 40.07271444145411], [116.14305232274667, 40.06231150684208], [116.16313670438304, 40.06152322130762]"
		//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --queryDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
		//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsLineStrings" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --queryDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
		//--onCluster "false" --approximateQuery "false" --queryOption "701" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "outputTopic" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd HH:mm:ss" --queryDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.005" --aggregate "SUM" --wType "TIME" --wInterval "10" --wStep "10" --uniformGridSize 25 --cellLengthMeters 50 --k "5" --trajDeletionThreshold 1000 --gridBBox1 "[115.50000, 39.60000, 117.60000, 40.91506]" --gridBBox2 "[-74.25540, 40.49843, -73.7000, 40.91506]" --trajIDSet "10007, 2560, 5261, 1743, 913" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.984416, 40.675882], [-73.984511, 40.675767], [-73.984719, 40.675867], [-73.984726, 40.67587], [-73.984718, 40.675881], [-73.984631, 40.675986], [-73.984416, 40.675882]" --queryLineString "[-73.984416, 40.675882], [-73.984511, 40.675767]" --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --kafkaBootStrapServers "150.82.97.204:9092" --queryPointSet "[[116.5, 40.5], [117.6, 40.5], [117.6, 41.4]], [[117.5, 40.5], [118.6, 40.5], [118.6, 41.4]]" --queryPolygonSet "[[[116.5, 40.5], [117.6, 40.5], [117.6, 41.4], [116.5, 41.4], [116.5, 40.5]]] , [[[117.5, 40.5], [118.6, 40.5], [118.6, 41.4], [117.5, 41.4], [117.5, 40.5]]] , [[[116.5, 40.5], [117.6, 40.5], [117.6, 41.4], [116.5, 41.4], [116.5, 40.5]]]" --queryLineStringSet "[[[116.5, 40.5], [117.6, 40.5], [117.6, 41.4], [116.5, 41.4]]] , [[[118.5, 40.5], [119.6, 40.5], [119.6, 41.4], [118.5, 41.4]]] , [[[116.5, 40.5], [117.6, 40.5], [117.6, 41.4], [116.5, 41.4]]]"
		// NYCBuildingsLineStrings
		//TaxiDriveGeoJSON_Live
		//NYCBuildingsPolygonsGeoJSON_Live
		//NYCBuildingsLineStringsGeoJSON_Live
		//--dateFormat "yyyy-MM-dd HH:mm:ss"
		//--dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

		//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "NYCBuildingsLineStringsGeoJSON_Live" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --queryDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --qGridMinX "-74.25540" --qGridMaxX "-73.70007" --qGridMinY "40.49843" --qGridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
		//--onCluster "false" --approximateQuery "false" --queryOption "106" --inputTopicName "TaxiDrive17MillionGeoJSON" --queryTopicName "NYCBuildingsPolygonsGeoJSON_Live" --outputTopicName "Latency8" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd HH:mm:ss" --queryDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "5" --wStep "3" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "115.50000" --gridMaxX "117.60000" --gridMinY "39.60000" --gridMaxY "41.10000" --qGridMinX "-74.25540" --qGridMaxX "-73.70007" --qGridMinY "40.49843" --qGridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[116.14319183444924, 40.07271444145411]" --queryPolygon "[116.14319183444924, 40.07271444145411], [116.14305232274667, 40.06231150684208], [116.16313670438304, 40.06152322130762], [116.14319183444924, 40.07271444145411]" --queryLineString "[116.14319183444924, 40.07271444145411], [116.14305232274667, 40.06231150684208], [116.16313670438304, 40.06152322130762]"
		//--onCluster "false" --approximateQuery "false" --queryOption "137" --inputTopicName "NYCBuildingsLineStrings" --queryTopicName "NYCBuildingsPolygonsGeoJSON_Live" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --queryDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --qGridMinX "-74.25540" --qGridMaxX "-73.70007" --qGridMinY "40.49843" --qGridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
		//--onCluster "false" --kafkaBootStrapServers "150.82.97.204:9092" --approximateQuery "false" --queryOption "1012" --inputTopicName "TaxiDriveGeoJSON_Live" --queryTopicName "Beijing_Polygons_Live" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd HH:mm:ss" --queryDateFormat "yyyy-MM-dd HH:mm:ss" --ordinaryStreamAttributes "[oID, timestamp]" --queryStreamAttributes "[doitt_id, lstmoddate]" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "3" --wStep "3" --uniformGridSize 100 --cellLengthMeters 50 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "115.50000" --gridMaxX "117.60000" --gridMinY "39.60000" --gridMaxY "40.91506" --qGridMinX "115.50000" --qGridMaxX "117.60000" --qGridMinY "39.60000" --qGridMaxY "41.10000" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"

		//ParameterTool parameters = ParameterTool.fromArgs(args);

		Params params = new Params();
		System.out.println(params);
		/* Cluster */
		boolean onCluster = params.clusterMode;
		String bootStrapServers = params.kafkaBootStrapServers;

		/* Input Stream1 */
		String inputTopicName = params.inputTopicName1;
		String inputFormat = params.inputFormat1;
		String dateFormatStr = params.dateFormatStr1;
		List<String> geoJSONSchemaAttr1 = params.geoJSONSchemaAttr1;
		List<String> ordinaryStreamAttributeNames = params.geoJSONSchemaAttr1;// default order of attributes: objectID, timestamp
		List<Integer> csvTsvSchemaAttr1 = params.csvTsvSchemaAttr1;
		List<Double> gridBBox1 = params.gridBBox1;
		int uniformGridSize = params.numGridCells1;
		double cellLengthMeters = params.cellLength1;
		String inputDelimiter1 = params.inputDelimiter1;
		String charset1 = params.charset1;

		/* Input Stream2 */
		String queryTopicName = params.inputTopicName2;
		String inputFormat2 = params.inputFormat2;
		String queryDateFormatStr = params.dateFormatStr2;
		List<String> queryStreamAttributeNames = params.geoJSONSchemaAttr2;
		List<Integer> csvTsvSchemaAttr2 = params.csvTsvSchemaAttr2;
		List<Double> gridBBox2 = params.gridBBox2;
		int uniformGridSize2 = params.numGridCells2;
		double cellLengthMeters2 = params.cellLength1;
		String inputDelimiter2 = params.inputDelimiter2;
		String charset2 = params.charset2;

		/* Stream Output */
		String outputTopicName = params.outputTopicName;
		String outputDelimiter = params.outputDelimiter;

		/* Query */
		int queryOption = params.queryOption;
		boolean approximateQuery = params.queryApproximate;
		double radius = params.queryRadius; // Default 10x10 Grid
		String aggregateFunction = params.queryAggregateFunction;    // "ALL", "SUM", "AVG", "MIN", "MAX" (Default = ALL)
		int k = params.queryK; // k denotes filter size in filter query
		int omegaDuration = params.queryOmegaDuration;
		Set<String> trajIDs = params.queryTrajIDSet;
		List<Coordinate> queryPointCoordinates = params.queryPoints;
		List<List<Coordinate>> queryPolygons = params.queryPolygons;
		List<List<Coordinate>> queryLineStrings = params.queryLineStrings;
		Long inactiveTrajDeletionThreshold = params.queryTrajDeletion;
		int allowedLateness = params.queryOutOfOrderTuples;

		/* Windows */
		String windowType = params.windowType;
		int windowSize = params.windowInterval;
		int windowSlideStep = params.windowStep;


		//String dataset = parameters.get("dataset"); // TDriveBeijing, ATCShoppingMall


		double gridMinX = gridBBox1.get(0);
		double gridMinY = gridBBox1.get(1);
		double gridMaxX = gridBBox1.get(2);
		double gridMaxY = gridBBox1.get(3);

		double qGridMinX = gridBBox2.get(0);
		double qGridMinY = gridBBox2.get(1);
		double qGridMaxX = gridBBox2.get(2);
		double qGridMaxY = gridBBox2.get(3);



		List<Coordinate> queryLineStringCoordinates = queryLineStrings.get(0);
		List<Coordinate> queryPolygonCoordinates = queryPolygons.get(0);
		List<List<Coordinate>> queryPointSetCoordinates = new ArrayList<>();
		queryPointSetCoordinates.add(queryPointCoordinates);
		List<List<List<Coordinate>>> queryPolygonSetCoordinates = new ArrayList<>();
		queryPolygonSetCoordinates.add(queryPolygons);
		List<List<List<Coordinate>>> queryLineStringSetCoordinates = new ArrayList<>();
		queryLineStringSetCoordinates.add(queryLineStrings);

		//String bootStrapServers;
		DateFormat inputDateFormat;
		DateFormat queryDateFormat;

		if(dateFormatStr.equals("null"))
			inputDateFormat = null;
		else
			inputDateFormat = new SimpleDateFormat(dateFormatStr);

		if(queryDateFormatStr.equals("null"))
			queryDateFormat = null;
		else
			queryDateFormat = new SimpleDateFormat(queryDateFormatStr);
		//inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // TDrive Dataset


		if (onCluster) {
			env = StreamExecutionEnvironment.getExecutionEnvironment();
			//bootStrapServers = "172.16.0.64:9092, 172.16.0.81:9092";

		}else{
			env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
			//env = StreamExecutionEnvironment.getExecutionEnvironment();
			//bootStrapServers = "localhost:9092";
			//bootStrapServers = "150.82.97.204:9092";
			// For testing spatial trajectory queries

			/*
			queryOption = 21;
			radius =  0.004;
			uniformGridSize = 50;
			//uniformGridSize = 200;
			windowSize = 5;
			windowSlideStep = 5;
			k = 10;
			//inputTopicName = "TaxiDrive17MillionGeoJSON";
			//inputTopicName = "NYCBuildingsPolygons";
			inputTopicName = "ATCShoppingMall";
			//inputTopicName = "ATCShoppingMall_CSV";
			outputTopicName = "outputTopicGeoFlink";
			inputFormat = "GeoJSON"; // CSV, GeoJSON
			inputDateFormat = null;
			 */
		}
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setParallelism(30);

		/*
		// Boundaries for Taxi Drive dataset
		double minX = 115.50000;     //X - East-West longitude
		double maxX = 117.60000;
		double minY = 39.60000;     //Y - North-South latitude
		double maxY = 41.10000;
		 */

		/*
		// Boundaries for NYC dataset
		double minX = -74.25540;     //X - East-West longitude
		double maxX = -73.70007;
		double minY = 40.49843;     //Y - North-South latitude
		double maxY = 40.91506;
		 */

		/*
		// Boundaries for test dataset
		double minX = 0;     //X - East-West longitude
		double maxX = 10;
		double minY = 0;     //Y - North-South latitude
		double maxY = 10;
		 */

		/*
		// Boundaries for ATC Shopping Mall dataset
		double minX = -41543.0;
		double maxX = 48431.0;
		double minY = -27825.0;
		double maxY = 24224.0;
		 */

		// Preparing Kafka Connection to Get Stream Tuples
		Properties kafkaProperties = new Properties();
		//kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
		kafkaProperties.setProperty("bootstrap.servers", bootStrapServers);
		kafkaProperties.setProperty("group.id", "messageStream");

		// Defining Grid
		UniformGrid uGrid;
		UniformGrid qGrid;

		/*
		Point queryPoint = new Point(-73.9857, 40.6789, uGrid);
		//Point queryPoint = new Point(116.414899, 39.920374, uGrid);

		// Polygon-Polygon Range Query
		ArrayList<Coordinate> queryPolygonCoordinates = new ArrayList<Coordinate>();
		queryPolygonCoordinates.add(new Coordinate(-73.984416, 40.675882));
		queryPolygonCoordinates.add(new Coordinate(-73.984511, 40.675767));
		queryPolygonCoordinates.add(new Coordinate(-73.984719, 40.675867));
		queryPolygonCoordinates.add(new Coordinate(-73.984726, 40.67587));
		queryPolygonCoordinates.add(new Coordinate(-73.984718, 40.675881));
		queryPolygonCoordinates.add(new Coordinate(-73.984631, 40.675986));
		queryPolygonCoordinates.add(new Coordinate(-73.984416, 40.675882));
		Polygon queryPolygon = new Polygon(queryPolygonCoordinates, uGrid);
		 */


		/*
		// ____ SIMULATE POLYGON STREAM____
		DataStream<Polygon> spatialPolygonStream = env.addSource(new SourceFunction<Polygon>() {
			@Override
			public void run(SourceFunction.SourceContext<Polygon> context) throws Exception {
				//while (true) { //comment loop for finite stream, uncomment for infinite stream
				Thread.sleep(500);
				context.collect(queryPolygon);
				Thread.sleep(3000); // increase time if no output
				//}
			}
			@Override
			public void cancel() {
			}
		});
		*/

		// Dataset-specific Parameters
		// TDriveBeijing, ATCShoppingMall, NYCBuildingsPolygon
		Set<Polygon> polygonSet;
		Set<Point> qPointSet;
		Set<Polygon> queryPolygonSet;
		Set<LineString> queryLineStringSet;
		Set<MultiPoint> multiPointSet = new HashSet<MultiPoint>();
		Set<MultiPolygon> multiPolygonSet = new HashSet<MultiPolygon>();
		Set<MultiLineString> multiLineStringSet = new HashSet<MultiLineString>();

		if(cellLengthMeters > 0) {
			uGrid = new UniformGrid(cellLengthMeters, gridMinX, gridMaxX, gridMinY, gridMaxY);
			qGrid = new UniformGrid(cellLengthMeters2, qGridMinX, qGridMaxX, qGridMinY, qGridMaxY);
		}else{
			uGrid = new UniformGrid(uniformGridSize, gridMinX, gridMaxX, gridMinY, gridMaxY);
			qGrid = new UniformGrid(uniformGridSize2, qGridMinX, qGridMaxX, qGridMinY, qGridMaxY);
		}

		qPointSet = Stream.of(new Point(queryPointCoordinates.get(0).x, queryPointCoordinates.get(0).y, uGrid)).collect(Collectors.toSet());

		List<List<Coordinate>> listCoordinatePolygon = new ArrayList<List<Coordinate>>();
		listCoordinatePolygon.add(queryPolygonCoordinates);
		queryPolygonSet = Stream.of(new Polygon(listCoordinatePolygon, uGrid)).collect(Collectors.toSet());

		polygonSet = queryPolygonSet;
		queryLineStringSet =  Stream.of(new LineString(queryLineStringCoordinates, uGrid)).collect(Collectors.toSet());

		for (List<Coordinate> l : queryPointSetCoordinates) {
			multiPointSet.add(new MultiPoint(l, uGrid));
		}
		for (List<List<Coordinate>> l : queryPolygonSetCoordinates) {
			List<List<List<Coordinate>>> list = new ArrayList<>();
			list.add(l);
			multiPolygonSet.add(new MultiPolygon(list, uGrid));
		}
		for (List<List<Coordinate>> l : queryLineStringSetCoordinates) {
			multiLineStringSet.add(new MultiLineString(null, l, uGrid));
		}

		/*
		if(dataset.equals("TDriveBeijing")) {
			// T-Drive Beijing
			minX = 115.50000;     //X - East-West longitude
			maxX = 117.60000;
			minY = 39.60000;     //Y - North-South latitude
			maxY = 41.10000;

			// Defining Grid
			uGrid = new UniformGrid(uniformGridSize, minX, maxX, minY, maxY);

			// setting set size for filter query
			if(k == 5)
				trajIDs = Stream.of("10007", "2560", "5261", "1743", "913").collect(Collectors.toSet());
			else if(k == 10)
				trajIDs = Stream.of("10007", "2560", "5261", "1743", "913", "1240", "2068", "2091", "3288", "6627").collect(Collectors.toSet());
			else
				trajIDs = new HashSet<>();

			ArrayList<Coordinate> queryPolygonCoord = new ArrayList<Coordinate>();
			queryPolygonCoord.add(new Coordinate(116.14319183444924, 40.07271444145411));
			queryPolygonCoord.add(new Coordinate(116.18473388691926, 40.035529388268685));
			queryPolygonCoord.add(new Coordinate(116.21666290761499, 39.86550816999926));
			queryPolygonCoord.add(new Coordinate(116.15177490885888, 39.83651509181427));
			queryPolygonCoord.add(new Coordinate(116.13907196730345, 39.86260941325255));
			queryPolygonCoord.add(new Coordinate(116.09375336469513, 39.994247317537756));
			queryPolygonCoord.add(new Coordinate(116.14319183444924, 40.07271444145411));
			queryPoly = new Polygon(queryPolygonCoord, uGrid);
			polygonSet = Stream.of(queryPoly).collect(Collectors.toSet());

			qPoint = new Point(116.14319183444924, 40.07271444145411, uGrid);
		}
		else if(dataset.equals("ATCShoppingMall")){

			// ATC Shopping Mall
			// Boundaries for ATC Shopping Mall dataset
			minX = -41543.0;
			maxX = 48431.0;
			minY = -27825.0;
			maxY = 24224.0;

			// Defining Grid
			uGrid = new UniformGrid(uniformGridSize, minX, maxX, minY, maxY);

			if(k == 5)
				trajIDs = Stream.of("9211800", "9320801", "9090500", "7282400", "10390100").collect(Collectors.toSet());
			else if(k == 10)
				trajIDs = Stream.of("9211800", "9320801", "9090500", "7282400", "10390100", "10350905", "9215801", "9230700", "9522100", "9495201").collect(Collectors.toSet());
			else
				trajIDs = new HashSet<>();

			ArrayList<Coordinate> queryPolygonCoord = new ArrayList<Coordinate>();
			queryPolygonCoord.add(new Coordinate(-39800, -25000));
			queryPolygonCoord.add(new Coordinate(-37500, -15000));
			queryPolygonCoord.add(new Coordinate(-15000, 0));
			queryPolygonCoord.add(new Coordinate(10000, 10000));
			queryPolygonCoord.add(new Coordinate(-39800, -25000));
			queryPoly = new Polygon(queryPolygonCoord, uGrid);
			polygonSet = Stream.of(queryPoly).collect(Collectors.toSet());

			qPoint = new Point(-37500, -15000, uGrid);
		}
		else if(dataset.equals("NYCBuildingsPolygons")){

			minX = -74.25540;     //X - East-West longitude
			maxX = -73.70007;
			minY = 40.49843;     //Y - North-South latitude
			maxY = 40.91506;

			// Defining Grid
			uGrid = new UniformGrid(uniformGridSize, minX, maxX, minY, maxY);

			if(k == 5)
				trajIDs = Stream.of("9211800", "9320801", "9090500", "7282400", "10390100").collect(Collectors.toSet());
			else if(k == 10)
				trajIDs = Stream.of("9211800", "9320801", "9090500", "7282400", "10390100", "10350905", "9215801", "9230700", "9522100", "9495201").collect(Collectors.toSet());
			else
				trajIDs = new HashSet<>();

			ArrayList<Coordinate> queryPolygonCoord = new ArrayList<Coordinate>();
			queryPoly = new Polygon(queryPolygonCoord, uGrid);
			polygonSet = Stream.of(queryPoly).collect(Collectors.toSet());

			qPoint = new Point(-74.0000, 40.72714, uGrid);
		}
		else{
			minX = -180;     //X - East-West longitude
			maxX = 180;
			minY = -90;     //Y - North-South latitude
			maxY = 90;

			// Defining Grid
			uGrid = new UniformGrid(uniformGridSize, minX, maxX, minY, maxY);

			// setting set size for filter query
			if(k == 5)
				trajIDs = Stream.of("10007", "2560", "5261", "1743", "913").collect(Collectors.toSet());
			else if(k == 10)
				trajIDs = Stream.of("10007", "2560", "5261", "1743", "913", "1240", "2068", "2091", "3288", "6627").collect(Collectors.toSet());
			else
				trajIDs = new HashSet<>();

			ArrayList<Coordinate> queryPolygonCoord = new ArrayList<Coordinate>();
			queryPolygonCoord.add(new Coordinate(116.14319183444924, 40.07271444145411));
			queryPolygonCoord.add(new Coordinate(116.18473388691926, 40.035529388268685));
			queryPolygonCoord.add(new Coordinate(116.21666290761499, 39.86550816999926));
			queryPolygonCoord.add(new Coordinate(116.15177490885888, 39.83651509181427));
			queryPolygonCoord.add(new Coordinate(116.13907196730345, 39.86260941325255));
			queryPolygonCoord.add(new Coordinate(116.09375336469513, 39.994247317537756));
			queryPolygonCoord.add(new Coordinate(116.14319183444924, 40.07271444145411));
			queryPoly = new Polygon(queryPolygonCoord, uGrid);
			polygonSet = Stream.of(queryPoly).collect(Collectors.toSet());

			qPoint = new Point(0, 0, uGrid);
		}
		*/
		// Generating stream
		DataStream inputStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
		//DataStream inputStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromLatest());

		// Converting GeoJSON,CSV stream to point spatial data stream
		//DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
		QueryConfiguration realtimeConf = new QueryConfiguration(QueryType.RealTime);
		realtimeConf.setApproximateQuery(approximateQuery);
		realtimeConf.setWindowSize(omegaDuration);


		QueryConfiguration realtimeNaiveConf = new QueryConfiguration(QueryType.RealTimeNaive);
		realtimeNaiveConf.setApproximateQuery(approximateQuery);
		realtimeNaiveConf.setWindowSize(omegaDuration);


		QueryConfiguration windowConf = new QueryConfiguration(QueryType.WindowBased);
		windowConf.setApproximateQuery(approximateQuery);
		windowConf.setAllowedLateness(allowedLateness);
		windowConf.setWindowSize(windowSize);
		windowConf.setSlideStep(windowSlideStep);

		switch(queryOption) {

			case 1: { // Range Query (Window-based) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(geoJSONStream, inputFormat, uGrid);
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//DataStream<Point> rNeighbors = RangeQuery.PointRangeQueryIncremental(spatialPointStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				DataStream<Point> rNeighbors = new PointPointRangeQuery(windowConf, uGrid).run(spatialPointStream, qPointSet, radius);
				//rNeighbors.print();
				//DataStream<Point> rNeighbors= RangeQuery.PointRangeQuery(spatialPointStream, qPoint, radius, uGrid, windowSize, windowSlideStep);  // better than equivalent GB approach
				//rNeighbors.print();
				break;}
			case 2: { // Range Query (Real-time) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream<Point> rNeighbors = new PointPointRangeQuery(realtimeConf, uGrid).run(spatialPointStream, qPointSet, radius);
				//rNeighbors.print();
				break;}


			case 6: { // Range Query (Window-based) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream<Point> rNeighbors = new PointPolygonRangeQuery(windowConf, uGrid).run(spatialPointStream, queryPolygonSet, radius);
				//rNeighbors.print();
				break;}
			case 7: { // Range Query (Real-time) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				polygonSet = HelperClass.generateQueryPolygons(k, gridMinX, gridMinY, gridMaxX, gridMaxY, uGrid);
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream<Point> rNeighbors = new PointPolygonRangeQuery(realtimeConf, uGrid).run(spatialPointStream, queryPolygonSet, radius);
				//rNeighbors.print();
				break;}

			case 70: { // Range Query (Real-time) - Point-Stream-Polygon-Query - Naive
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream<Point> rNeighbors = new PointPolygonRangeQuery(realtimeNaiveConf, uGrid).run(spatialPointStream, queryPolygonSet, radius);
				//rNeighbors.print();
				break;}

			case 8: { // Range Query (Window-based) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream<Long> rNeighbors = new PointPolygonRangeQuery(windowConf, uGrid).queryLatency(spatialPointStream, ((Polygon[])queryPolygonSet.toArray())[0], radius);
				//rNeighbors.print();
				rNeighbors.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkLong(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;}
			case 9: { // Range Query (Real-time) - Point-Stream-Polygon-Query - Latency
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream<Long> rNeighbors = new PointPolygonRangeQuery(realtimeConf, uGrid).queryLatency(spatialPointStream, ((Polygon[])queryPolygonSet.toArray())[0], radius);
				//rNeighbors.print();
				rNeighbors.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkLong(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

				break;}

			case 11: { // Range Query (Window-based) - Point-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream<Point> rNeighbors = new PointLineStringRangeQuery(windowConf, uGrid).run(spatialPointStream, queryLineStringSet, radius);
				//rNeighbors.print();
				break;}
			case 12: { // Range Query (Real-time) - Point-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream<Point> rNeighbors = new PointLineStringRangeQuery(realtimeConf, uGrid).run(spatialPointStream, queryLineStringSet, radius);
				//rNeighbors.print();
				break;}


			case 16: { // Range Query (Window-based) - Polygon-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "16" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1,"timestamp", "oID", uGrid);
				DataStream<Polygon> pointPolygonRangeQueryOutput = new PolygonPointRangeQuery(windowConf, uGrid).run(spatialPolygonStream, qPointSet, radius);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 17: { // Range Query (Real-time) - Polygon-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Polygon> pointPolygonRangeQueryOutput = new PolygonPointRangeQuery(realtimeConf, uGrid).run(spatialPolygonStream, qPointSet, radius);
				//pointPolygonRangeQueryOutput.print();
				break;}


			case 21: { // Range Query (Window-based) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Polygon> pointPolygonRangeQueryOutput = new PolygonPolygonRangeQuery(windowConf, uGrid).run(spatialPolygonStream, queryPolygonSet, radius);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 22: { // Range Query (Real-time) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Polygon> pointPolygonRangeQueryOutput = new PolygonPolygonRangeQuery(realtimeConf, uGrid).run(spatialPolygonStream, queryPolygonSet, radius);
				//pointPolygonRangeQueryOutput.print();
				break;}


			case 26: { // Range Query (Window-based) - Polygon-Stream-Polygon-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Polygon> pointPolygonRangeQueryOutput = new PolygonLineStringRangeQuery(windowConf, uGrid).run(spatialPolygonStream, queryLineStringSet, radius);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 27: { // Range Query (Real-time) - Polygon-Stream-Polygon-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Polygon> pointPolygonRangeQueryOutput = new PolygonLineStringRangeQuery(realtimeConf, uGrid).run(spatialPolygonStream, queryLineStringSet, radius);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 31: { // Range Query (Window-based) - LineString-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "16" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1,"timestamp", "oID", uGrid);
				DataStream<LineString> pointPolygonRangeQueryOutput = new LineStringPointRangeQuery(windowConf, uGrid).run(spatialLineStringStream, qPointSet, radius);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 32: { // Range Query (Real-time) - LineString-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<LineString> pointPolygonRangeQueryOutput = new LineStringPointRangeQuery(realtimeConf, uGrid).run(spatialLineStringStream, qPointSet, radius);
				//pointPolygonRangeQueryOutput.print();
				break;}


			case 36: { // Range Query (Window-based) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<LineString> pointPolygonRangeQueryOutput = new LineStringPolygonRangeQuery(windowConf, uGrid).run(spatialLineStringStream, queryPolygonSet, radius);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 37: { // Range Query (Real-time) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<LineString> pointPolygonRangeQueryOutput = new LineStringPolygonRangeQuery(realtimeConf, uGrid).run(spatialLineStringStream, queryPolygonSet, radius);
				//pointPolygonRangeQueryOutput.print();
				break;}


			case 41: { // Range Query (Window-based) - LineString-Stream-Polygon-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<LineString> pointPolygonRangeQueryOutput = new LineStringLineStringRangeQuery(windowConf, uGrid).run(spatialLineStringStream, queryLineStringSet, radius);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 42: { // Range Query (Real-time) - LineString-Stream-Polygon-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<LineString> pointPolygonRangeQueryOutput = new LineStringLineStringRangeQuery(realtimeConf, uGrid).run(spatialLineStringStream, queryLineStringSet, radius);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 51: { // KNN (Window-based) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = new PointPointKNNQuery(windowConf, uGrid).run(spatialPointStream, ((Point[])qPointSet.toArray())[0], radius, k);
				kNNPQStream.print();
				break;}

			case 52: { // KNN (Real-time) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = new PointPointKNNQuery(realtimeConf, uGrid).run(spatialPointStream, ((Point[])qPointSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}


			case 56: { // KNN (Window-based) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = new PointPolygonKNNQuery(windowConf, uGrid).run(spatialPointStream, ((Polygon[])queryPolygonSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}
			case 57: { // KNN (Real-time) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = new PointPolygonKNNQuery(realtimeConf, uGrid).run(spatialPointStream, ((Polygon[])queryPolygonSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}
			case 570: { // KNN (Real-time Naive) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = new PointPolygonKNNQuery(realtimeNaiveConf, uGrid).run(spatialPointStream, ((Polygon[])queryPolygonSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}
			case 58: { // KNN (Window-based) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream <Long> kNNPQStream = new PointPolygonKNNQuery(windowConf, uGrid).runLatency(spatialPointStream, ((Polygon[])queryPolygonSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				kNNPQStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkLong(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;}
			case 59: { // KNN (Real-time) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream <Long> kNNPQStream = new PointPolygonKNNQuery(realtimeConf, uGrid).runLatency(spatialPointStream, ((Polygon[])queryPolygonSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				kNNPQStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkLong(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;}


			case 61: { // KNN (Window-based) - Point-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = new PointLineStringKNNQuery(windowConf, uGrid).run(spatialPointStream, ((LineString[])queryLineStringSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}
			case 62: { // KNN (Real-time) - Point-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = new PointLineStringKNNQuery(realtimeConf, uGrid).run(spatialPointStream, ((LineString[])queryLineStringSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}


			case 66: { // KNN (Window-based) - Polygon-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "16" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> kNNPQStream = new PolygonPointKNNQuery(windowConf, uGrid).run(spatialPolygonStream, ((Point[])qPointSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}
			case 67: { // KNN (Real-time) - Polygon-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> kNNPQStream = new PolygonPointKNNQuery(realtimeConf, uGrid).run(spatialPolygonStream, ((Point[])qPointSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}


			case 71: { // KNN (Window-based) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> kNNPQStream = new PolygonPolygonKNNQuery(windowConf, uGrid).run(spatialPolygonStream, ((Polygon[])queryPolygonSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}

			case 72: { // KNN (Real-time) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> kNNPQStream = new PolygonPolygonKNNQuery(realtimeConf, uGrid).run(spatialPolygonStream, ((Polygon[])queryPolygonSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}


			case 76: { // KNN (Window-based) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> kNNPQStream = new PolygonLineStringKNNQuery(windowConf, uGrid).run(spatialPolygonStream, ((LineString[])queryLineStringSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}
			case 77: { // Range Query (Real-time) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> kNNPQStream = new PolygonLineStringKNNQuery(realtimeConf, uGrid).run(spatialPolygonStream, ((LineString[])queryLineStringSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}

			case 81: { // KNN (Window-based) - LineString-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> kNNPQStream = new LineStringPointKNNQuery(windowConf, uGrid).run(spatialLineStringStream, ((Point[])qPointSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}
			case 82: { // KNN (Real-time) - LineString-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> kNNPQStream = new LineStringPointKNNQuery(realtimeConf, uGrid).run(spatialLineStringStream, ((Point[])qPointSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}


			case 86: { // KNN (Window-based) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> kNNPQStream = new LineStringPolygonKNNQuery(windowConf, uGrid).run(spatialLineStringStream, ((Polygon[])queryPolygonSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}
			case 87: { // KNN (Real-time) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> kNNPQStream = new LineStringPolygonKNNQuery(realtimeConf, uGrid).run(spatialLineStringStream, ((Polygon[])queryPolygonSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}


			case 91: { // KNN (Window-based) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> kNNPQStream = new LineStringLineStringKNNQuery(windowConf, uGrid).run(spatialLineStringStream, ((LineString[])queryLineStringSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}
			case 92: { // KNN (Real-time) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> kNNPQStream = new LineStringLineStringKNNQuery(realtimeConf, uGrid).run(spatialLineStringStream, ((LineString[])queryLineStringSet.toArray())[0], radius, k);
				//kNNPQStream.print();
				break;}


			case 101: { // Join (Window-base) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, Point>> spatialJoinStream = new PointPointJoinQuery(windowConf, uGrid, qGrid).run(spatialPointStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}

			case 102: { // Join (Real-time) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, Point>> spatialJoinStream =  new PointPointJoinQuery(realtimeConf, uGrid, qGrid).run(spatialPointStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}

			case 1020: { // Join (Window-base) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, Point>> spatialJoinStream = new PointPointJoinQuery(realtimeNaiveConf, uGrid, qGrid).run(spatialPointStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}


			case 106: { // Join (Window-base) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, Polygon>> spatialJoinStream = new PointPolygonJoinQuery(windowConf, uGrid, qGrid).run(spatialPointStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}
			case 107: { // Join (Real-time) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, Polygon>> spatialJoinStream = new PointPolygonJoinQuery(realtimeConf, uGrid, qGrid).run(spatialPointStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}
			case 1070: { // Join (Real-time Naive) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, Polygon>> spatialJoinStream = new PointPolygonJoinQuery(realtimeNaiveConf, uGrid, qGrid).run(spatialPointStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}
			case 108: { // Join (Window-base) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Long> spatialJoinStream = new PointPolygonJoinQuery(windowConf, uGrid, qGrid).runLatency(spatialPointStream, queryStream, radius);
				//spatialJoinStream.print();
				spatialJoinStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkLong(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;}
			case 109: { // Join (Real-time) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Long> spatialJoinStream = new PointPolygonJoinQuery(realtimeConf, uGrid, qGrid).runLatency(spatialPointStream, queryStream, radius);
				//spatialJoinStream.print();
				spatialJoinStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkLong(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;}

			case 111: { // Join (Window-base) - Point-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<LineString> queryStream = Deserialization.TrajectoryStreamLineString(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, LineString>> spatialJoinStream = new PointLineStringJoinQuery(windowConf, uGrid, qGrid).run(spatialPointStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}
			case 112: { // Join (Real-time) - Point-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<LineString> queryStream = Deserialization.TrajectoryStreamLineString(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, LineString>> spatialJoinStream = new PointLineStringJoinQuery(realtimeConf, uGrid, qGrid).run(spatialPointStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}


			case 116: { // Join (Window-base) - Polygon-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "16" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Polygon, Point>> spatialJoinStream = new PolygonPointJoinQuery(windowConf, uGrid, qGrid).run(spatialPolygonStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}
			case 117: { // Join (Real-time) - Polygon-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Polygon, Point>> spatialJoinStream = new PolygonPointJoinQuery(realtimeConf, uGrid, qGrid).run(spatialPolygonStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}


			case 121: { // Join (Window-base) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Polygon, Polygon>> spatialJoinStream = new PolygonPolygonJoinQuery(windowConf, uGrid, qGrid).run(spatialPolygonStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}

			case 122: { // Join (Real-time) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Polygon, Polygon>> spatialJoinStream = new PolygonPolygonJoinQuery(realtimeConf, uGrid, qGrid).run(spatialPolygonStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}


			case 126: { // Join (Window-base) - Polygon-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<LineString> queryStream = Deserialization.TrajectoryStreamLineString(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Polygon, LineString>> spatialJoinStream = new PolygonLineStringJoinQuery(windowConf, uGrid, qGrid).run(spatialPolygonStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}
			case 127: { // Join (Real-time) - Polygon-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<LineString> queryStream = Deserialization.TrajectoryStreamLineString(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Polygon, LineString>> spatialJoinStream = new PolygonLineStringJoinQuery(realtimeConf, uGrid, qGrid).run(spatialPolygonStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}

			case 131: { // Join (Window-base) - LineString-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<LineString, Point>> spatialJoinStream = new LineStringPointJoinQuery(windowConf, uGrid, qGrid).run(spatialLineStringStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}
			case 132: { // Join (Real-time) - LineString-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<LineString, Point>> spatialJoinStream = new LineStringPointJoinQuery(realtimeConf, uGrid, qGrid).run(spatialLineStringStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}

			case 136: { // Join (Window-base) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<LineString, Polygon>> spatialJoinStream = new LineStringPolygonJoinQuery(windowConf, uGrid, qGrid).run(spatialLineStringStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}
			case 137: { // Join (Real-time) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);
				DataStream<Tuple2<LineString, Polygon>> spatialJoinStream = new LineStringPolygonJoinQuery(realtimeConf, uGrid, qGrid).run(spatialLineStringStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}


			case 141: { // Join (Window-base) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<LineString> queryStream = Deserialization.TrajectoryStreamLineString(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<LineString, LineString>> spatialJoinStream = new LineStringLineStringJoinQuery(windowConf, uGrid, qGrid).run(spatialLineStringStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}
			case 142: { // Join (Real-time) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<LineString> queryStream = Deserialization.TrajectoryStreamLineString(geoJSONQueryStream, inputFormat, queryDateFormat, inputDelimiter1, "timestamp", "oID", qGrid);

				DataStream<Tuple2<LineString, LineString>> spatialJoinStream = new LineStringLineStringJoinQuery(realtimeConf, uGrid, qGrid).run(spatialLineStringStream, queryStream, radius);
				//spatialJoinStream.print();
				break;}


				/*
			case 2: { // KNN (Grid based - fixed radius)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, uGrid);
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(geoJSONStream, inputFormat, uGrid);
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(csvStream, "CSV", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = KNNQuery.SpatialKNNQuery(spatialPointStream, qPoint, radius, k, windowSize, windowSlideStep, uGrid);
				kNNPQStream.print();
				break;}
			case 3: {
				break;}
			case 4: { // Spatial Join (Grid-based)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, uGrid);
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(geoJSONStream, inputFormat, uGrid);
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(csvStream, "CSV", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, inputDateFormat, uGrid);
				//DataStream<Point> queryStream = SpatialStream.PointStream(geoJSONQueryStream, inputFormat, uGrid);
				//DataStream<Tuple2<String, String>> spatialJoinStream = JoinQuery.SpatialJoinQuery(spatialPointStream, queryStream, radius, windowSize, windowSlideStep, uGrid);
				//spatialJoinStream.print();
				break;}
			case 5:{ // Range Query (Point-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, uGrid);
				//spatialPolygonStream.print();
				// Point-Polygon Range Query
				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;
			}
			case 6:{ // Range Query (Polygon-Polygon)

				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());

				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(geoJSONStream, inputFormat, uGrid);
				DataStream<Polygon> polygonPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, queryPoly, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				polygonPolygonRangeQueryOutput.print();
				break;
			}
			case 7:{ // KNN Query (Point-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(geoJSONStream, inputFormat, uGrid);
				// The output stream contains time-window boundaries (starting and ending time) and a Priority Queue containing topK query neighboring polygons
				//DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> pointPolygonkNNQueryOutput = KNNQuery.SpatialKNNQuery(spatialPolygonStream, qPoint, radius, k, uGrid, windowSize, windowSlideStep);
				//pointPolygonkNNQueryOutput.print();
				break;
			}
			case 8:{ // KNN Query (Polygon-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(geoJSONStream, inputFormat, uGrid);
				//DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> pointPolygonkNNQueryOutput = KNNQuery.SpatialKNNQuery(spatialPolygonStream, queryPoly, radius, k, uGrid, windowSize, windowSlideStep);
				//pointPolygonkNNQueryOutput.print();
				break;
			}
			case 9:{ // Join Query (Point-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(geoJSONStream, inputFormat, uGrid);
				//spatialPolygonStream.print();

				//Generating query stream TaxiDrive17MillionGeoJSON
				//DataStream geoJSONQueryPointStream  = env.addSource(new FlinkKafkaConsumer<>("NYCFoursquareCheckIns", new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream geoJSONQueryPointStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromEarliest());
				DataStream<Point> queryPointStream = Deserialization.PointStream(geoJSONQueryPointStream, "GeoJSON", uGrid);
				//geoJSONQueryPointStream.print();

				//---Spatial Join using Neighboring Layers---
//				DataStream<Tuple2<String, String>> spatialJoinStream = JoinQuery.SpatialJoinQuery(spatialPolygonStream, queryPointStream, radius, uGrid, windowSize, windowSlideStep);
//				spatialJoinStream.print();

				//----Optimized Spatial Join using Candidate and Guaranteed Neighbors---
//				DataStream<Tuple2<String, String>> spatialJoinStreamOptimized = JoinQuery.SpatialJoinQueryOptimized(spatialPolygonStream, queryPointStream, radius, uGrid, windowSize, windowSlideStep);
//				spatialJoinStreamOptimized.print();
				break;
			}
			case 10:{ // Join Query (Polygon-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON, CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(geoJSONStream, "GeoJSON", uGrid);
				//spatialPolygonStream.print();

				//Generating query stream TaxiDrive17MillionGeoJSON
				//DataStream geoJSONQueryPolygonStream  = env.addSource(new FlinkKafkaConsumer<>("NYCFoursquareCheckIns", new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream geoJSONQueryPolygonStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromEarliest());
				DataStream<Polygon> queryPolygonStream = Deserialization.PolygonStream(geoJSONQueryPolygonStream, "GeoJSON", uGrid);
				//queryPolygonStream.print();

				//---Spatial Join using Neighboring Layers----
//				DataStream<Tuple2<String, String>> spatialJoinStream = JoinQuery.SpatialJoinQuery(spatialPolygonStream, queryPolygonStream,  windowSlideStep, windowSize, radius, uGrid);
//				spatialJoinStream.print();

				//----Optimized Spatial Join using Candidate and Guaranteed Neighbors---
//				DataStream<Tuple2<String, String>> spatialJoinStreamOptimized = JoinQuery.SpatialJoinQueryOptimized(spatialPolygonStream, queryPolygonStream,  windowSlideStep, windowSize, radius, uGrid);
//				spatialJoinStreamOptimized.print();
				break;
			}

				 */
			case 201:{ // TFilterQuery Real-time
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);

				//spatialTrajectoryStream.print();
				//DataStream<Point> outputStream = TFilterQuery.TIDSpatialFilterQuery(spatialTrajectoryStream, trajIDs);

				DataStream<Point> outputStream = (DataStream<Point>)new PointTFilterQuery(realtimeConf).run(spatialTrajectoryStream, trajIDs);

				//outputStream.print();
				//outputStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkPoint(queryOption, outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 202:{ // TFilterQuery Windowed
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				new PointTFilterQuery(windowConf).run(spatialTrajectoryStream, trajIDs);
				break;
			}
			case 203:{ // TRangeQuery Real-time
				//System.out.println(qPolygonSet);
				//System.out.println(qPolygonSet.size());
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				polygonSet = HelperClass.generateQueryPolygons(k, gridMinX, gridMinY, gridMaxX, gridMaxY, uGrid);
				DataStream<Point> outputStream = (DataStream<Point>)new PointPolygonTRangeQuery(realtimeConf).run(spatialTrajectoryStream, polygonSet);
				//outputStream.print();
				//outputStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkPoint(queryOption, outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 2030:{ // TRangeQuery Real-time Naive
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				polygonSet = HelperClass.generateQueryPolygons(k, gridMinX, gridMinY, gridMaxX, gridMaxY, uGrid);
				//Naive
				DataStream<Point> outputStream = TRangeQuery.realTimeNaive(polygonSet, spatialTrajectoryStream);
				break;
			}
			case 204:{ // TRangeQuery Windowed
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				new PointPolygonTRangeQuery(windowConf).run(spatialTrajectoryStream, polygonSet);//.print();
				break;
			}
			case 205:{ // TStatsQuery_ Real-time
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream<Tuple5<String, Double, Long, Double, Long>> outputStream = (DataStream<Tuple5<String, Double, Long, Double, Long>>)new PointTStatsQuery(realtimeConf).run(spatialTrajectoryStream, trajIDs);
				//outputStream.print();
				outputStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkTuple5(queryOption, outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 206:{ // TStatsQuery_ Windowed
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				new PointTStatsQuery(windowConf).run(spatialTrajectoryStream, trajIDs);
				break;
			}
			case 207:{ // TSpatialHeatmapAggregateQuery Real-time
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream<Tuple4<String, Integer, HashMap<String, Long>, Long>> outputStream = (DataStream<Tuple4<String, Integer, HashMap<String, Long>, Long>>)new PointTAggregateQuery(realtimeConf).run(spatialTrajectoryStream, aggregateFunction, "", inactiveTrajDeletionThreshold);
				//outputStream.print();
				outputStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkTuple4(queryOption, outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 208:{ // TAggregateQuery Windowed
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				new PointTAggregateQuery(windowConf).run(spatialTrajectoryStream, aggregateFunction, windowType, Long.valueOf(0));
				break;
			}
			case 209:{ // TSpatialJoinQuery Real-time
				// Generating query stream
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream queryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromLatest());
				DataStream<Point> spatialQueryStream = Deserialization.TrajectoryStream(queryStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream<Tuple2<Point, Point>> outputStream =(DataStream<Tuple2<Point, Point>>)new PointPointTJoinQuery(realtimeConf, uGrid).run(spatialTrajectoryStream, spatialQueryStream, radius);
				//new PointPointTJoinQuery(windowConf, uGrid).run(spatialTrajectoryStream, spatialQueryStream, radius).print();

				// Self Stream Join
				//new PointPointTJoinQuery(realtimeConf, uGrid).runSingle(spatialTrajectoryStream, radius);//.print();

				// Naive
				//TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, spatialQueryStream, radius, windowSize, allowedLateness);

				break;
			}
			case 2090:{ // TSpatialJoinQuery Real-time Naive
				// Generating query stream
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream queryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromLatest());
				DataStream<Point> spatialQueryStream = Deserialization.TrajectoryStream(queryStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				// Naive
				DataStream<Tuple2<Point, Point>> outputStream = TJoinQuery.realTimeJoinNaive(spatialTrajectoryStream, spatialQueryStream, radius, windowSize, allowedLateness);
				break;
			}

			case 210:{ // TSpatialJoinQuery Windowed
				// Generating query stream
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream queryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromLatest());
				DataStream<Point> spatialQueryStream = Deserialization.TrajectoryStream(queryStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				new PointPointTJoinQuery(windowConf, uGrid).run(spatialTrajectoryStream, spatialQueryStream, radius);//.print();

				// Self Stream Join
				new PointPointTJoinQuery(windowConf, uGrid).runSingle(spatialTrajectoryStream, radius);//.print();

				// Naive
				//TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, spatialQueryStream, radius, windowSize);

				break;
			}
			case 211:{ // TSpatialKNNQuery Real-time
				//System.out.println("qPoint " + qPoint);
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//Real-time kNN
				DataStream<Tuple2<Point, Double>> outputStream = (DataStream<Tuple2<Point, Double>>)new PointPointTKNNQuery(realtimeConf, uGrid).run(spatialTrajectoryStream, ((Point[])qPointSet.toArray())[0], radius, k); //.print();
				//outputStream.print();
				break;
			}
			case 2011:{ // TSpatialKNNQuery Real-time Naive
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				// Naive
				DataStream<Tuple2<Point, Double>> outputStream = (DataStream<Tuple2<Point, Double>>)new PointPointTKNNQuery(realtimeConf, uGrid).runNaive(spatialTrajectoryStream, ((Point[])qPointSet.toArray())[0], radius, k); //.print();
				//outputStream.print();
				break;
			}
			case 212:{ // TSpatialKNNQuery Windowed
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//Window-based kNN
				new PointPointTKNNQuery(windowConf, uGrid).run(spatialTrajectoryStream, ((Point[])qPointSet.toArray())[0], radius, k);

				break;
			}
			case 401: { // Point (GeoJSON Point)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.PointStream(geoJSONStream, "GeoJSON", inputDelimiter1, csvTsvSchemaAttr1, uGrid);
				spatialPointStream.print();
//				DataStream<Point> rNeighbors= RangeQuery.SpatialRangeQuery(spatialPointStream, qPoint, radius, windowSize, windowSlideStep, uGrid);  // better than equivalent GB approach
//				rNeighbors.print();

				break;}
			case 402:{ // Polygon (GeoJSON Polygon)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(geoJSONStream, "GeoJSON", uGrid);
				spatialPolygonStream.print();
				// Point-Polygon Range Query
//				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.SpatialRangeQuery(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep);
//				pointPolygonRangeQueryOutput.print();
				break;
			}
			case 403:{ // LineString (GeoJSON LineString)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.LineStringStream(geoJSONStream, "GeoJSON", uGrid);
				spatialLineStringStream.print();
				break;
			}
			case 404:{ // GeometryCollection (GeoJSON GeometryCollection no Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<GeometryCollection> spatialStream = Deserialization.GeometryCollectionStream(geoJSONStream, "GeoJSON", uGrid);
				spatialStream.print();
				break;
			}
			case 405:{ // MultiPoint (GeoJSON MultiPoint no Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<MultiPoint> spatialStream = Deserialization.MultiPointStream(geoJSONStream, "GeoJSON", uGrid);
				spatialStream.print();
				break;
			}
			case 406:{ // Range Query (Point-LineString)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to linestring spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.LineStringStream(geoJSONStream, "GeoJSON", uGrid);
				spatialLineStringStream.print();
				// Point-Polygon Range Query
				DataStream<LineString> pointLineStringRangeQueryOutput = new LineStringPointRangeQuery(windowConf, uGrid).run(spatialLineStringStream, qPointSet, radius);
				pointLineStringRangeQueryOutput.print();
				break;
			}
			case 501: { // Point (WKT CSV Point)
				DataStream csvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.PointStream(csvStream, "WKT", inputDelimiter1, csvTsvSchemaAttr1, uGrid);
				spatialPointStream.print();
//				DataStream<Point> rNeighbors= new PointPointRangeQuery(windowConf, uGrid).run(spatialPointStream, qPoint, radius);  // better than equivalent GB approach
//				rNeighbors.print();
				break;}
			case 502:{ // Polygon (WKT CSV Polygon)
				DataStream csvStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(csvStream, "WKT", uGrid);
				spatialPolygonStream.print();
				// Point-Polygon Range Query
//				DataStream<Polygon> pointPolygonRangeQueryOutput = new PolygonPointRangeQuery(windowConf, uGrid).run(spatialPolygonStream, qPoint, radius);
//				pointPolygonRangeQueryOutput.print();
				break;
			}
			case 503:{ // LineString (WKT CSV LineString)
				DataStream csvStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.LineStringStream(csvStream, "WKT", uGrid);
				spatialLineStringStream.print();
				break;
			}
			case 504:{ // GeometryCollection (WKT CSV GeometryCollection no Timestamp)
				DataStream csvStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<GeometryCollection> spatialLineStringStream = Deserialization.GeometryCollectionStream(csvStream, "WKT", uGrid);
				spatialLineStringStream.print();
				break;
			}
			case 505:{ // MultiPoint (WKT CSV MultiPoint no Timestamp)
				DataStream csvStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<MultiPoint> spatialMultiPointStream = Deserialization.MultiPointStream(csvStream, "WKT", uGrid);
				spatialMultiPointStream.print();
				break;
			}
			case 506: { // Point (NOT WKT CSV Point)
				DataStream csvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.PointStream(csvStream, "CSV", inputDelimiter1, csvTsvSchemaAttr1, uGrid);
				spatialPointStream.print();
				break;}
			case 601: { // Range Query (WKT TSV Point)
				DataStream tsvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.PointStream(tsvStream, "WKT", inputDelimiter1, csvTsvSchemaAttr1, uGrid);
				spatialPointStream.print();
//				DataStream<Point> rNeighbors= new PointPointRangeQuery(windowConf, uGrid).run(spatialPointStream, qPoint, radius);  // better than equivalent GB approach
//				rNeighbors.print();
				break;}
			case 602:{ // Range Query (WKT TSV Polygon)
				DataStream tsvStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,TSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(tsvStream, "WKT", uGrid);
				spatialPolygonStream.print();
				// Point-Polygon Range Query
//				DataStream<Polygon> pointPolygonRangeQueryOutput = new PolygonPointRangeQuery(windowConf, uGrid).run(spatialPolygonStream, qPoint, radius);
//				pointPolygonRangeQueryOutput.print();
				break;
			}
			case 603:{ // Range Query (WKT TSV LineString)
				DataStream tsvStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.LineStringStream(tsvStream, "WKT", uGrid);
				spatialLineStringStream.print();
				break;
			}
			case 604:{ // GeometryCollection (WKT TSV GeometryCollection no Timestamp)
				DataStream tsvStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,TSV stream to polygon spatial data stream
				DataStream<GeometryCollection> spatialLineStringStream = Deserialization.GeometryCollectionStream(tsvStream, "WKT", uGrid);
				spatialLineStringStream.print();
				break;
			}
			case 605:{ // MultiPoint (WKT TSV GeometryCollection no Timestamp)
				DataStream tsvStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,TSV stream to polygon spatial data stream
				DataStream<MultiPoint> spatialMultiPointStream = Deserialization.MultiPointStream(tsvStream, "WKT", uGrid);
				spatialMultiPointStream.print();
				break;
			}
			case 606: { // Range Query (NOT WKT TSV Point)
				DataStream tsvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.PointStream(tsvStream, "TSV", inputDelimiter1, csvTsvSchemaAttr1, uGrid);
				spatialPointStream.print();
				break;}
			case 701:{ // TFilterQuery (GeoJSON Point Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(geoJSONStream, "GeoJSON", inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PointToGeoJSONOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 702:{ // TFilterQuery (GeoJSON Polygon Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Polygon> spatialTrajectoryStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, "GeoJSON", inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PolygonToGeoJSONOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 703:{ // TFilterQuery (GeoJSON LineString Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<LineString> spatialTrajectoryStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, "GeoJSON", inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.LineStringToGeoJSONOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 704:{ // GeometryCollection (GeoJSON GeometryCollection Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<GeometryCollection> spatialTrajectoryStream = Deserialization.TrajectoryStreamGeometryCollection(geoJSONStream, "GeoJSON", inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.GeometryCollectionToGeoJSONOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 705:{ // MultiPoint (GeoJSON MultiPoint Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<MultiPoint> spatialTrajectoryStream = Deserialization.TrajectoryStreamMultiPoint(geoJSONStream, "GeoJSON", inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.MultiPointToGeoJSONOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 801:{ // Point (WKT CSV Point Timestamp)
				DataStream csvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(csvStream, "WKT", inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PointToWKTOutputSchema("kafka", inputDateFormat, outputDelimiter), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 802:{ // Polygon (WKT CSV Polygon Timestamp)
				DataStream csvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				DataStream<Polygon> spatialTrajectoryStream = Deserialization.TrajectoryStreamPolygon(csvStream, "WKT", inputDateFormat, inputDelimiter1, null, null,uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PolygonToWKTOutputSchema("kafka", inputDateFormat, outputDelimiter), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 803:{ // LineString (WKT CSV LineString Timestamp)
				DataStream csvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				DataStream<LineString> spatialTrajectoryStream = Deserialization.TrajectoryStreamLineString(csvStream, "WKT", inputDateFormat, inputDelimiter1, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.LineStringToWKTOutputSchema("kafka", inputDateFormat, outputDelimiter), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 804:{ // GeometryCollection (WKT CSV GeometryCollection Timestamp)
				DataStream csvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				DataStream<GeometryCollection> spatialTrajectoryStream = Deserialization.TrajectoryStreamGeometryCollection(csvStream, "WKT", inputDateFormat, inputDelimiter1, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.GeometryCollectionToWKTOutputSchema("kafka", inputDateFormat, outputDelimiter), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 805:{ // MultiPoint (WKT CSV MultiPoint Timestamp)
				DataStream csvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				DataStream<MultiPoint> spatialTrajectoryStream = Deserialization.TrajectoryStreamMultiPoint(csvStream, "WKT", inputDateFormat, inputDelimiter1, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.MultiPointToWKTOutputSchema("kafka", inputDateFormat, outputDelimiter), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 806:{ // Point (NOT WKT CSV Point Timestamp)
				DataStream csvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(csvStream, "CSV", inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PointToCSVTSVOutputSchema("kafka", inputDateFormat, outputDelimiter, csvTsvSchemaAttr1), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 901:{ // Point (WKT TSV Point Timestamp)
				DataStream tsvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(tsvStream, "WKT", inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PointToWKTOutputSchema("kafka", inputDateFormat, outputDelimiter), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 902:{ // Polygon (WKT TSV Polygon Timestamp)
				DataStream tsvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				DataStream<Polygon> spatialTrajectoryStream = Deserialization.TrajectoryStreamPolygon(tsvStream, "WKT", inputDateFormat, inputDelimiter1, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PolygonToWKTOutputSchema("kafka", inputDateFormat, outputDelimiter), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 903:{ // LineString (WKT TSV LineString Timestamp)
				DataStream tsvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				DataStream<LineString> spatialTrajectoryStream = Deserialization.TrajectoryStreamLineString(tsvStream, "WKT", inputDateFormat, inputDelimiter1, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.LineStringToWKTOutputSchema("kafka", inputDateFormat, outputDelimiter), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 904:{ // GeometryCollection (WKT TSV GeometryCollection Timestamp)
				DataStream tsvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				DataStream<GeometryCollection> spatialTrajectoryStream = Deserialization.TrajectoryStreamGeometryCollection(tsvStream, "WKT", inputDateFormat, inputDelimiter1, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.GeometryCollectionToWKTOutputSchema("kafka", inputDateFormat, outputDelimiter), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 905:{ // MultiPoint (WKT TSV MultiPoint Timestamp)
				DataStream tsvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				DataStream<MultiPoint> spatialTrajectoryStream = Deserialization.TrajectoryStreamMultiPoint(tsvStream, "WKT", inputDateFormat, inputDelimiter1, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.MultiPointToWKTOutputSchema("kafka", inputDateFormat, outputDelimiter), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 906:{ // Point (NOT WKT TSV Point Timestamp)
				DataStream tsvStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new SimpleStringSchema(Charset.forName(charset1)), kafkaProperties).setStartFromEarliest());
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(tsvStream, "TSV", inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PointToCSVTSVOutputSchema("kafka", inputDateFormat, outputDelimiter, csvTsvSchemaAttr1), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 1001:{ // Shapefile (Point)
				Path path = new Path("D:\\testData\\point\\W01-05_GML\\W01-05-g_Dam.shp");
				ShapeFileInputFormat<Polygon> shapeFileInputFormat = new ShapeFileInputFormat<Polygon>(path, uGrid);
				TypeInformation<Polygon> typeInfo = TypeInformation.of(Polygon.class);
				DataStream<Polygon> stream =  env.createInput(shapeFileInputFormat, typeInfo);
				stream.print();
				break;
			}
			case 1002:{ // Shapefile (Polygon)
				Path path = new Path("D:\\testData\\polygon\\japan_ver821.shp");
				ShapeFileInputFormat<Polygon> shapeFileInputFormat = new ShapeFileInputFormat<Polygon>(path, uGrid);
				TypeInformation<Polygon> typeInfo = TypeInformation.of(Polygon.class);
				DataStream<Polygon> stream =  env.createInput(shapeFileInputFormat, typeInfo);
				stream.print();
				break;
			}
			case 1003:{ // Shapefile (LineString)
				Path path = new Path("D:\\testData\\linestring\\A37-15_01_GML\\A37-15_PeninsulaRoadCirculation_01.shp");
//				Path path = new Path("D:\\testData\\linestring\\C23-06_05_GML\\C23-06_05-g_Coastline.shp");
				ShapeFileInputFormat<MultiLineString> shapeFileInputFormat = new ShapeFileInputFormat<MultiLineString>(path, uGrid);
				TypeInformation<MultiLineString> typeInfo = TypeInformation.of(MultiLineString.class);
				DataStream<MultiLineString> stream =  env.createInput(shapeFileInputFormat, typeInfo);
				stream.print();
				break;
			}
			case 99:{ // For testing with synthetic data
				ArrayList<Point> points = new ArrayList<Point>();

				//Getting the current date
				Date date = new Date();
				// Getting the random number
				Random r = new Random();
				// Creating dummy trajectories
				for (int i = 1, j = 1; i < 1000; i++) {

					if(i%4 == 0)
						j = 1;

					points.add(new Point( Integer.toString(j), r.nextInt(10), r.nextInt(10), date.getTime() + i*100, uGrid));
					j++;

					break;
				}


				/*
				ArrayList<Coordinate> queryPolygonCoord = new ArrayList<Coordinate>();
				queryPolygonCoord.add(new Coordinate(1, 1));
				queryPolygonCoord.add(new Coordinate(5, 9));
				queryPolygonCoord.add(new Coordinate(9, 1));
				queryPolygonCoord.add(new Coordinate(1, 1));
				Polygon queryPoly = new Polygon(queryPolygonCoord, uGrid);

				Set<String> trajIDs = Set.<String>of("1", "4");
				Set<Polygon> polygonSet = Set.<Polygon>of(queryPoly);
				Point qPoint = new Point(5.0,5.0,uGrid);

				// creating a stream from points
				DataStream<Point> ds = env.fromCollection(points);

				//TFilterQuery.TIDSpatialFilterQuery(ds, trajIDs).print();
				//TFilterQuery.TIDSpatialFilterQuery(ds, trajIDs, 1, 1).print();
				//TRangeQuery.TSpatialRangeQuery(ds, polygonSet).print();
				//TRangeQuery.TSpatialRangeQuery(ds, polygonSet, 1, 1).print();
				//TStatsQuery_.TSpatialStatsQuery(ds, trajIDs).print();
				//TStatsQuery_.TSpatialStatsQuery(ds, trajIDs, 5, 1).print();
				//TAggregateQuery.TSpatialHeatmapAggregateQuery(ds, "MAX").print();
				//TAggregateQuery.TSpatialHeatmapAggregateQuery(ds, "MAX", "TIME", 5, 5).print();
				//TJoinQuery.TSpatialJoinQuery(ds,ds,1,1,uGrid).print();
				//TKNNQuery.TSpatialRangeQuery(ds,qPoint, 10, 3, 1,1, uGrid).print();
				 */
				break;
			}
			case 1010:{
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				//spatialPointStream.print();
				//trajIDs = Stream.of("1001", "2059", "1741", "1415", "2565").collect(Collectors.toSet());
				trajIDs = Stream.of("0").collect(Collectors.toSet());
				//Set<String> trajIDSet = Stream.of("1001", "2059", "1741", "1415", "2565").collect(Collectors.toSet());
				DataStream<Tuple2<String, Double>> CellStayTime = StayTime.CellStayTime(spatialPointStream, trajIDs, allowedLateness, windowSize, windowSlideStep, uGrid);
				//CellStayTime.print();
				//CellStayTime.addSink(new FlinkKafkaProducer<>(outputTopicName, new Serialization.PointToGeoJSONOutputSchema(outputTopicName, inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 1011:{
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Polygon> spatialStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, "timestamp", "oID", uGrid);
				//spatialStream.print();
				//trajIDs = Stream.of("1001", "2059", "1741", "1415", "2565").collect(Collectors.toSet());
				//trajIDs = Stream.of("0").collect(Collectors.toSet());
				//Set<String> trajIDSet = Stream.of("1001", "2059", "1741", "1415", "2565").collect(Collectors.toSet());
				Set<String> trajectoryIDSet = new HashSet<String>();
				DataStream<Tuple2<String, Integer>> CellSensorRangeIntersection = StayTime.CellSensorRangeIntersection(spatialStream, trajectoryIDSet, allowedLateness, windowSize, windowSlideStep, uGrid);
				CellSensorRangeIntersection.print();
				break;
			}
			case 1012: {
				// Point stream
				DataStream pointStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromLatest());
				//DataStream pointStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).
				//
				//
				//
				// ());
				//pointStream.print();
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(pointStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, ordinaryStreamAttributeNames.get(1), ordinaryStreamAttributeNames.get(0), uGrid);
				//spatialPointStream.print();
				Set<String> trajectoryIDSetPoint = new HashSet<String>();

				// Polygon stream
				DataStream polygonStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromLatest());
				//DataStream polygonStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				 //Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(polygonStream, inputFormat, inputDateFormat, inputDelimiter1, queryStreamAttributeNames.get(1), queryStreamAttributeNames.get(0), uGrid);
				//spatialPolygonStream.print();
				Set<String> trajectoryIDSetPolygon = new HashSet<String>();
				DataStream<Tuple4<String, Long, Long, Double>> normalizedCellStayTime = StayTime.normalizedCellStayTime(spatialPointStream, trajectoryIDSetPoint, spatialPolygonStream, trajectoryIDSetPolygon, allowedLateness, windowSize, windowSlideStep, uGrid);
				//normalizedCellStayTime.print();
				normalizedCellStayTime.addSink(new FlinkKafkaProducer<>(outputTopicName, new StayTime.normalizedStayTimeOutputSchema(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			/*
			DataStream<Tuple4<String, Long, Long, Double>> normalizedCellStayTime_ = spatialPointStream.map(new MapFunction<Point, Tuple4<String, Long, Long, Double>>() {
				@Override
				public Tuple4<String, Long, Long, Double> map(Point point) throws Exception {
					return Tuple4.of(point.objID, point.timeStampMillisec, point.ingestionTime, 500.95);
				}
			});
			 */

			case 2000:{ //DEIM Check-In

				HashMap<String, Integer> roomCapacities = new HashMap<>();
				roomCapacities.put("A", 100);
				roomCapacities.put("B", 100);
				roomCapacities.put("C", 100);

				DataStream pointStream = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromLatest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				//inputDateFormat = "12/25/2020 17:24:36 +0900";
				inputDateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss z"); // TDrive Dataset
				inputFormat = "JSON";
				DataStream<Point> deimCheckInStream = Deserialization.TrajectoryStream(pointStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, ordinaryStreamAttributeNames.get(1), ordinaryStreamAttributeNames.get(0), uGrid);


				CheckIn.CheckInQuery(deimCheckInStream, roomCapacities, 24).print();
				//CheckIn.CheckInQuery(deimCheckInStream, roomCapacities, 24).print();
			}
			default:
				System.out.println("Input Unrecognized. Please select option from 1-10.");
		}

		// Execute program
		env.execute("Geo Flink");
	}




}
