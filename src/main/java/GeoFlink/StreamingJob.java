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
import GeoFlink.spatialStreams.*;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.shaded.guava18.com.google.common.collect.Multimap;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.locationtech.jts.geom.Coordinate;
import scala.Serializable;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
		//NYCBuildingsLineStrings
		//TaxiDriveGeoJSON_Live
		//NYCBuildingsPolygonsGeoJSON_Live
		//NYCBuildingsLineStringsGeoJSON_Live
		//--dateFormat "yyyy-MM-dd HH:mm:ss"
		//--dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

		//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "NYCBuildingsLineStringsGeoJSON_Live" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --queryDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --qGridMinX "-74.25540" --qGridMaxX "-73.70007" --qGridMinY "40.49843" --qGridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
		//--onCluster "false" --approximateQuery "false" --queryOption "106" --inputTopicName "TaxiDrive17MillionGeoJSON" --queryTopicName "NYCBuildingsPolygonsGeoJSON_Live" --outputTopicName "Latency8" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd HH:mm:ss" --queryDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "5" --wStep "3" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "115.50000" --gridMaxX "117.60000" --gridMinY "39.60000" --gridMaxY "41.10000" --qGridMinX "-74.25540" --qGridMaxX "-73.70007" --qGridMinY "40.49843" --qGridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[116.14319183444924, 40.07271444145411]" --queryPolygon "[116.14319183444924, 40.07271444145411], [116.14305232274667, 40.06231150684208], [116.16313670438304, 40.06152322130762], [116.14319183444924, 40.07271444145411]" --queryLineString "[116.14319183444924, 40.07271444145411], [116.14305232274667, 40.06231150684208], [116.16313670438304, 40.06152322130762]"
		//--onCluster "false" --approximateQuery "false" --queryOption "137" --inputTopicName "NYCBuildingsLineStrings" --queryTopicName "NYCBuildingsPolygonsGeoJSON_Live" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --queryDateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --qGridMinX "-74.25540" --qGridMaxX "-73.70007" --qGridMinY "40.49843" --qGridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
		//--onCluster "false" --kafkaBootStrapServers "150.82.97.204:9092" --approximateQuery "false" --queryOption "1012" --inputTopicName "TaxiDriveGeoJSON_Live" --queryTopicName "Beijing_Polygons_Live" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd HH:mm:ss" --queryDateFormat "yyyy-MM-dd HH:mm:ss" --ordinaryStreamAttributes "[oID, timestamp]" --queryStreamAttributes "[doitt_id, lstmoddate]" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "3" --wStep "3" --uniformGridSize 100 --cellLengthMeters 50 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "115.50000" --gridMaxX "117.60000" --gridMinY "39.60000" --gridMaxY "40.91506" --qGridMinX "115.50000" --qGridMaxX "117.60000" --qGridMinY "39.60000" --qGridMaxY "41.10000" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
		

		ParameterTool parameters = ParameterTool.fromArgs(args);

		int queryOption = Integer.parseInt(parameters.get("queryOption"));
		String inputTopicName = parameters.get("inputTopicName");
		String queryTopicName = parameters.get("queryTopicName");
		String outputTopicName = parameters.get("outputTopicName");
		String inputFormat = parameters.get("inputFormat");
		String dateFormatStr = parameters.get("dateFormat");
		String queryDateFormatStr = parameters.get("queryDateFormat");
		String aggregateFunction = parameters.get("aggregate");  // "ALL", "SUM", "AVG", "MIN", "MAX" (Default = ALL)
		double radius = Double.parseDouble(parameters.get("radius")); // Default 10x10 Grid
		int uniformGridSize = Integer.parseInt(parameters.get("uniformGridSize"));
		double cellLengthMeters = Double.parseDouble(parameters.get("cellLengthMeters"));
		int windowSize = Integer.parseInt(parameters.get("wInterval"));
		int windowSlideStep = Integer.parseInt(parameters.get("wStep"));
		String windowType = parameters.get("wType");
		int k = Integer.parseInt(parameters.get("k")); // k denotes filter size in filter query
		boolean onCluster = Boolean.parseBoolean(parameters.get("onCluster"));
		String bootStrapServers = parameters.get("kafkaBootStrapServers");
		boolean approximateQuery = Boolean.parseBoolean(parameters.get("approximateQuery"));
		//String dataset = parameters.get("dataset"); // TDriveBeijing, ATCShoppingMall
		Long inactiveTrajDeletionThreshold = Long.parseLong(parameters.get("trajDeletionThreshold"));
		int allowedLateness = Integer.parseInt(parameters.get("outOfOrderAllowedLateness"));
		int omegaJoinDurationSeconds = Integer.parseInt(parameters.get("omegaJoinDuration"));

		double gridMinX = Double.parseDouble(parameters.get("gridMinX"));
		double gridMaxX = Double.parseDouble(parameters.get("gridMaxX"));
		double gridMinY = Double.parseDouble(parameters.get("gridMinY"));
		double gridMaxY = Double.parseDouble(parameters.get("gridMaxY"));

		double qGridMinX = Double.parseDouble(parameters.get("qGridMinX"));
		double qGridMaxX = Double.parseDouble(parameters.get("qGridMaxX"));
		double qGridMinY = Double.parseDouble(parameters.get("qGridMinY"));
		double qGridMaxY = Double.parseDouble(parameters.get("qGridMaxY"));

		String trajIDSet = parameters.get("trajIDSet");
		List<Coordinate> queryPointCoordinates = HelperClass.getCoordinates(parameters.get("queryPoint"));
		List<Coordinate> queryLineStringCoordinates = HelperClass.getCoordinates(parameters.get("queryLineString"));
		List<Coordinate> queryPolygonCoordinates = HelperClass.getCoordinates(parameters.get("queryPolygon"));

		List<String> ordinaryStreamAttributeNames = HelperClass.getParametersArray(parameters.get("ordinaryStreamAttributes")); // default order of attributes: objectID, timestamp
		List<String> queryStreamAttributeNames = HelperClass.getParametersArray(parameters.get("queryStreamAttributes"));

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

		double minX;
		double maxX;
		double minY;
		double maxY;

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
//		kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
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
		Set<String> trajIDs;
		Set<Polygon> polygonSet;
		Point qPoint;
		Polygon queryPolygon;
		LineString queryLineString;

		if(cellLengthMeters > 0) {
			uGrid = new UniformGrid(cellLengthMeters, gridMinX, gridMaxX, gridMinY, gridMaxY);
			qGrid = new UniformGrid(cellLengthMeters, qGridMinX, qGridMaxX, qGridMinY, qGridMaxY);
		}else{
			uGrid = new UniformGrid(uniformGridSize, gridMinX, gridMaxX, gridMinY, gridMaxY);
			qGrid = new UniformGrid(uniformGridSize, qGridMinX, qGridMaxX, qGridMinY, qGridMaxY);
		}



		String[] trajID = trajIDSet.split("\\s*,\\s*");
		trajIDs = Stream.of(trajID).collect(Collectors.toSet());

		qPoint = new Point(queryPointCoordinates.get(0).x, queryPointCoordinates.get(0).y, uGrid);

		List<List<Coordinate>> listCoordinatePolygon = new ArrayList<List<Coordinate>>();
		listCoordinatePolygon.add(queryPolygonCoordinates);
		queryPolygon = new Polygon(listCoordinatePolygon, uGrid);
		polygonSet = Stream.of(queryPolygon).collect(Collectors.toSet());

		queryLineString = new LineString(queryLineStringCoordinates, uGrid);




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

		switch(queryOption) {

			case 1: { // Range Query (Window-based) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(geoJSONStream, inputFormat, uGrid);
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//DataStream<Point> rNeighbors = RangeQuery.PointRangeQueryIncremental(spatialPointStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				DataStream<Point> rNeighbors = RangeQuery.PointRangeQuery(spatialPointStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//rNeighbors.print();
				//DataStream<Point> rNeighbors= RangeQuery.PointRangeQuery(spatialPointStream, qPoint, radius, uGrid, windowSize, windowSlideStep);  // better than equivalent GB approach
				//rNeighbors.print();
				break;}
			case 2: { // Range Query (Real-time) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Point> rNeighbors = RangeQuery.PointRangeQuery(spatialPointStream, qPoint, radius, uGrid, approximateQuery);
				//rNeighbors.print();
				break;}


			case 6: { // Range Query (Window-based) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Point> rNeighbors = RangeQuery.PointRangeQuery(spatialPointStream, queryPolygon, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//rNeighbors.print();
				break;}
			case 7: { // Range Query (Real-time) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Point> rNeighbors = RangeQuery.PointRangeQuery(spatialPointStream, queryPolygon, radius, uGrid, approximateQuery);
				//rNeighbors.print();
				break;}
			case 8: { // Range Query (Window-based) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Long> rNeighbors = RangeQuery.PointRangeQueryLatency(spatialPointStream, queryPolygon, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//rNeighbors.print();
				rNeighbors.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkLong(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;}
			case 9: { // Range Query (Real-time) - Point-Stream-Polygon-Query - Latency
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Long> rNeighbors = RangeQuery.PointRangeQueryLatency(spatialPointStream, queryPolygon, radius, uGrid, approximateQuery);
				//rNeighbors.print();
				rNeighbors.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkLong(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

				break;}

			case 11: { // Range Query (Window-based) - Point-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Point> rNeighbors = RangeQuery.PointRangeQuery(spatialPointStream, queryLineString, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//rNeighbors.print();
				break;}
			case 12: { // Range Query (Real-time) - Point-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Point> rNeighbors = RangeQuery.PointRangeQuery(spatialPointStream, queryLineString, radius, uGrid, approximateQuery);
				//rNeighbors.print();
				break;}


			case 16: { // Range Query (Window-based) - Polygon-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "16" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat,"timestamp", "oID", uGrid);
				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 17: { // Range Query (Real-time) - Polygon-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, qPoint, radius, uGrid, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}


			case 21: { // Range Query (Window-based) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, queryPolygon, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 22: { // Range Query (Real-time) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, queryPolygon, radius, uGrid, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}


			case 26: { // Range Query (Window-based) - Polygon-Stream-Polygon-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, queryLineString, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 27: { // Range Query (Real-time) - Polygon-Stream-Polygon-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, queryLineString, radius, uGrid, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 31: { // Range Query (Window-based) - LineString-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "16" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<LineString> pointPolygonRangeQueryOutput = RangeQuery.LineStringRangeQuery(spatialLineStringStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 32: { // Range Query (Real-time) - LineString-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<LineString> pointPolygonRangeQueryOutput = RangeQuery.LineStringRangeQuery(spatialLineStringStream, qPoint, radius, uGrid, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}


			case 36: { // Range Query (Window-based) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<LineString> pointPolygonRangeQueryOutput = RangeQuery.LineStringRangeQuery(spatialLineStringStream, queryPolygon, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 37: { // Range Query (Real-time) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<LineString> pointPolygonRangeQueryOutput = RangeQuery.LineStringRangeQuery(spatialLineStringStream, queryPolygon, radius, uGrid, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}


			case 41: { // Range Query (Window-based) - LineString-Stream-Polygon-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<LineString> pointPolygonRangeQueryOutput = RangeQuery.LineStringRangeQuery(spatialLineStringStream, queryLineString, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQueryIncremental(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 42: { // Range Query (Real-time) - LineString-Stream-Polygon-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<LineString> pointPolygonRangeQueryOutput = RangeQuery.LineStringRangeQuery(spatialLineStringStream, queryLineString, radius, uGrid, approximateQuery);
				//pointPolygonRangeQueryOutput.print();
				break;}

			case 51: { // KNN (Window-based) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = KNNQuery.PointKNNQuery(spatialPointStream, qPoint, radius, k, uGrid, windowSize, windowSlideStep, allowedLateness);
				//kNNPQStream.print();
				break;}

			case 52: { // KNN (Real-time) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = KNNQuery.PointKNNQuery(spatialPointStream, qPoint, radius, k, uGrid, omegaJoinDurationSeconds, allowedLateness);
				//kNNPQStream.print();
				break;}


			case 56: { // KNN (Window-based) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = KNNQuery.PointKNNQuery(spatialPointStream, queryPolygon, radius, k, windowSize, windowSlideStep, uGrid, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}
			case 57: { // KNN (Real-time) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = KNNQuery.PointKNNQuery(spatialPointStream, queryPolygon, radius, k, omegaJoinDurationSeconds, uGrid, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}
			case 58: { // KNN (Window-based) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream <Long> kNNPQStream = KNNQuery.PointKNNQueryLatency(spatialPointStream, queryPolygon, radius, k, windowSize, windowSlideStep, uGrid, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				kNNPQStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkLong(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;}
			case 59: { // KNN (Real-time) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream <Long> kNNPQStream = KNNQuery.PointKNNQueryLatency(spatialPointStream, queryPolygon, radius, k, omegaJoinDurationSeconds, uGrid, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				kNNPQStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkLong(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;}


			case 61: { // KNN (Window-based) - Point-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = KNNQuery.PointKNNQuery(spatialPointStream, queryLineString, radius, k, windowSize, windowSlideStep, uGrid, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}
			case 62: { // KNN (Real-time) - Point-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = KNNQuery.PointKNNQuery(spatialPointStream, queryLineString, radius, k, omegaJoinDurationSeconds, uGrid, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}


			case 66: { // KNN (Window-based) - Polygon-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "16" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> kNNPQStream = KNNQuery.PolygonKNNQuery(spatialPolygonStream, qPoint, radius, k, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}
			case 67: { // KNN (Real-time) - Polygon-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> kNNPQStream = KNNQuery.PolygonKNNQuery(spatialPolygonStream, qPoint, radius, k, uGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}


			case 71: { // KNN (Window-based) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> kNNPQStream = KNNQuery.PolygonKNNQuery(spatialPolygonStream, queryPolygon, radius, k, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}

			case 72: { // KNN (Real-time) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> kNNPQStream = KNNQuery.PolygonKNNQuery(spatialPolygonStream, queryPolygon, radius, k, uGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}


			case 76: { // KNN (Window-based) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> kNNPQStream = KNNQuery.PolygonKNNQuery(spatialPolygonStream, queryLineString, radius, k, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}
			case 77: { // Range Query (Real-time) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> kNNPQStream = KNNQuery.PolygonKNNQuery(spatialPolygonStream, queryLineString, radius, k, uGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}

			case 81: { // KNN (Window-based) - LineString-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> kNNPQStream = KNNQuery.LineStringKNNQuery(spatialLineStringStream, qPoint, radius, k, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}
			case 82: { // KNN (Real-time) - LineString-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> kNNPQStream = KNNQuery.LineStringKNNQuery(spatialLineStringStream, qPoint, radius, k, uGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}


			case 86: { // KNN (Window-based) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> kNNPQStream = KNNQuery.LineStringKNNQuery(spatialLineStringStream, queryPolygon, radius, k, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}
			case 87: { // KNN (Real-time) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> kNNPQStream = KNNQuery.LineStringKNNQuery(spatialLineStringStream, queryPolygon, radius, k, uGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}


			case 91: { // KNN (Window-based) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> kNNPQStream = KNNQuery.LineStringKNNQuery(spatialLineStringStream, queryLineString, radius, k, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}
			case 92: { // KNN (Real-time) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<LineString, Double>>>> kNNPQStream = KNNQuery.LineStringKNNQuery(spatialLineStringStream, queryLineString, radius, k, uGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
				//kNNPQStream.print();
				break;}


			case 101: { // Join (Window-base) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, Point>> spatialJoinStream = JoinQuery.PointJoinQuery(spatialPointStream, queryStream, radius, windowSize, windowSlideStep, uGrid, qGrid, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}
			case 102: { // Join (Real-time) - Point-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, Point>> spatialJoinStream = JoinQuery.PointJoinQuery(spatialPointStream, queryStream, radius, omegaJoinDurationSeconds, uGrid, qGrid, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}


			case 106: { // Join (Window-base) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, Polygon>> spatialJoinStream = JoinQuery.PointJoinQuery(spatialPointStream, queryStream, uGrid, qGrid, radius, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}
			case 107: { // Join (Real-time) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, Polygon>> spatialJoinStream = JoinQuery.PointJoinQuery(spatialPointStream, queryStream, uGrid, qGrid, radius, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}
			case 108: { // Join (Window-base) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Long> spatialJoinStream = JoinQuery.PointJoinQueryLatency(spatialPointStream, queryStream, uGrid, qGrid, radius, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				spatialJoinStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkLong(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;}
			case 109: { // Join (Real-time) - Point-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Long> spatialJoinStream = JoinQuery.PointJoinQueryLatency(spatialPointStream, queryStream, uGrid, qGrid, radius, omegaJoinDurationSeconds, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				spatialJoinStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkLong(outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;}

			case 111: { // Join (Window-base) - Point-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<LineString> queryStream = Deserialization.TrajectoryStreamLineString(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, LineString>> spatialJoinStream = JoinQuery.PointJoinQuery(spatialPointStream, queryStream, radius, uGrid, qGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}
			case 112: { // Join (Real-time) - Point-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<LineString> queryStream = Deserialization.TrajectoryStreamLineString(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Point, LineString>> spatialJoinStream = JoinQuery.PointJoinQuery(spatialPointStream, queryStream, radius, uGrid, qGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}


			case 116: { // Join (Window-base) - Polygon-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "16" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Polygon, Point>> spatialJoinStream = JoinQuery.PolygonJoinQuery(spatialPolygonStream, queryStream, radius, windowSize, windowSlideStep, uGrid, qGrid, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}
			case 117: { // Join (Real-time) - Polygon-Stream-Point-Query
				//--onCluster "false" --approximateQuery "false" --queryOption "17" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744], [-73.98452330316861, 40.67563064195701]" --queryLineString "[-73.98452330316861, 40.67563064195701], [-73.98776303794413, 40.671603874732455], [-73.97826680869485, 40.666980275860936], [-73.97297380718484, 40.67347172572744]"
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Polygon, Point>> spatialJoinStream = JoinQuery.PolygonJoinQuery(spatialPolygonStream, queryStream, radius, omegaJoinDurationSeconds, uGrid, qGrid, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}


			case 121: { // Join (Window-base) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Polygon, Polygon>> spatialJoinStream = JoinQuery.PolygonJoinQuery(spatialPolygonStream, queryStream, uGrid, qGrid, radius, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}

			case 122: { // Join (Real-time) - Polygon-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Polygon, Polygon>> spatialJoinStream = JoinQuery.PolygonJoinQuery(spatialPolygonStream, queryStream, uGrid, qGrid, radius, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}


			case 126: { // Join (Window-base) - Polygon-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<LineString> queryStream = Deserialization.TrajectoryStreamLineString(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Polygon, LineString>> spatialJoinStream = JoinQuery.PolygonJoinQuery(spatialPolygonStream, queryStream, radius, uGrid, qGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}
			case 127: { // Join (Real-time) - Polygon-Stream-LineString-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<LineString> queryStream = Deserialization.TrajectoryStreamLineString(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<Polygon, LineString>> spatialJoinStream = JoinQuery.PolygonJoinQuery(spatialPolygonStream, queryStream, radius, uGrid, qGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}

			case 131: { // Join (Window-base) - LineString-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<LineString, Point>> spatialJoinStream = JoinQuery.LineStringJoinQuery(spatialLineStringStream, queryStream, radius, windowSize, windowSlideStep, uGrid, qGrid, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}
			case 132: { // Join (Real-time) - LineString-Stream-Point-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = Deserialization.TrajectoryStream(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<LineString, Point>> spatialJoinStream = JoinQuery.LineStringJoinQuery(spatialLineStringStream, queryStream, radius, omegaJoinDurationSeconds, uGrid, qGrid, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}

			case 136: { // Join (Window-base) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<LineString, Polygon>> spatialJoinStream = JoinQuery.LineStringJoinQuery(spatialLineStringStream, queryStream, uGrid, qGrid, radius, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}
			case 137: { // Join (Real-time) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Polygon> queryStream = Deserialization.TrajectoryStreamPolygon(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);
				DataStream<Tuple2<LineString, Polygon>> spatialJoinStream = JoinQuery.LineStringJoinQuery(spatialLineStringStream, queryStream, uGrid, qGrid, radius, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}


			case 141: { // Join (Window-base) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<LineString> queryStream = Deserialization.TrajectoryStreamLineString(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<LineString, LineString>> spatialJoinStream = JoinQuery.LineStringJoinQuery(spatialLineStringStream, queryStream, radius, uGrid, qGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//spatialJoinStream.print();
				break;}
			case 142: { // Join (Real-time) - LineString-Stream-Polygon-Query
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<LineString> queryStream = Deserialization.TrajectoryStreamLineString(geoJSONQueryStream, inputFormat, queryDateFormat, "timestamp", "oID", qGrid);

				DataStream<Tuple2<LineString, LineString>> spatialJoinStream = JoinQuery.LineStringJoinQuery(spatialLineStringStream, queryStream, radius, uGrid, qGrid, omegaJoinDurationSeconds, allowedLateness, approximateQuery);
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
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Point> outputStream = TFilterQuery.TIDSpatialFilterQuery(spatialTrajectoryStream, trajIDs);
				//outputStream.print();
				outputStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkPoint(queryOption, outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 202:{ // TFilterQuery Windowed
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				TFilterQuery.TIDSpatialFilterQuery(spatialTrajectoryStream, trajIDs, windowSize, windowSlideStep);
				break;
			}
			case 203:{ // TRangeQuery Real-time
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Point> outputStream = TRangeQuery.TSpatialRangeQuery(spatialTrajectoryStream, polygonSet);
				//Naive
				//DataStream<Point> outputStream = TRangeQuery.TSpatialRangeQuery(polygonSet, spatialTrajectoryStream);

				//outputStream.print();
				outputStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkPoint(queryOption, outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 204:{ // TRangeQuery Windowed
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				TRangeQuery.TSpatialRangeQuery(spatialTrajectoryStream, polygonSet, windowSize, windowSlideStep, allowedLateness);//.print();
				break;
			}
			case 205:{ // TStatsQuery Real-time
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple5<String, Double, Long, Double, Long>> outputStream = TStatsQuery.TSpatialStatsQuery(spatialTrajectoryStream, trajIDs);
				//outputStream.print();
				outputStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkTuple5(queryOption, outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 206:{ // TStatsQuery Windowed
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				TStatsQuery.TSpatialStatsQuery(spatialTrajectoryStream, trajIDs, windowSize, windowSlideStep);
				break;
			}
			case 207:{ // TSpatialHeatmapAggregateQuery Real-time
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream<Tuple4<String, Integer, HashMap<String, Long>, Long>> outputStream = TAggregateQuery.TSpatialHeatmapAggregateQuery(spatialTrajectoryStream, aggregateFunction, inactiveTrajDeletionThreshold);
				//outputStream.print();
				outputStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkTuple4(queryOption, outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 208:{ // TAggregateQuery Windowed
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				TAggregateQuery.TSpatialHeatmapAggregateQuery(spatialTrajectoryStream, aggregateFunction, windowType, windowSize, windowSlideStep);
				break;
			}
			case 209:{ // TSpatialJoinQuery Real-time
				// Generating query stream
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream queryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromLatest());
				DataStream<Point> spatialQueryStream = Deserialization.TrajectoryStream(queryStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, spatialQueryStream, radius, omegaJoinDurationSeconds, allowedLateness, uGrid);
				//TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, spatialQueryStream, radius, windowSize, windowSlideStep, allowedLateness, uGrid).print();

				// Self Stream Join
				//TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, radius, omegaJoinDurationSeconds, allowedLateness, uGrid);//.print();

				// Naive
				//TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, spatialQueryStream, radius, windowSize);

				break;
			}
			case 210:{ // TSpatialJoinQuery Windowed
				// Generating query stream
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				DataStream queryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromLatest());
				DataStream<Point> spatialQueryStream = Deserialization.TrajectoryStream(queryStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, spatialQueryStream, radius, windowSize, windowSlideStep, allowedLateness, uGrid);//.print();

				// Self Stream Join
				TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, radius, omegaJoinDurationSeconds, allowedLateness, uGrid);//.print();

				// Naive
				//TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, spatialQueryStream, radius, windowSize);

				break;
			}
			case 211:{ // TSpatialKNNQuery Real-time
				//System.out.println("qPoint " + qPoint);
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Real-time kNN
				TKNNQuery.TSpatialKNNQuery(spatialTrajectoryStream, qPoint, radius, k, omegaJoinDurationSeconds, allowedLateness, uGrid); //.print();
				// Naive
				//TKNNQuery.TSpatialKNNQuery(spatialTrajectoryStream, qPoint, radius, k, windowSize, windowSlideStep, allowedLateness);

				break;
			}
			case 212:{ // TSpatialKNNQuery Windowed
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(inputStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
				//Window-based kNN
				TKNNQuery.TSpatialKNNQuery(spatialTrajectoryStream, qPoint, radius, k, windowSize, windowSlideStep, allowedLateness, uGrid);

				break;
			}
			case 401: { // Range Query (GeoJSON Point)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.PointStream(geoJSONStream, "GeoJSON", uGrid);
				spatialPointStream.print();
//				DataStream<Point> rNeighbors= RangeQuery.SpatialRangeQuery(spatialPointStream, qPoint, radius, windowSize, windowSlideStep, uGrid);  // better than equivalent GB approach
//				rNeighbors.print();

				break;}
			case 402:{ // Range Query (GeoJSON Polygon)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(geoJSONStream, "GeoJSON", uGrid);
				spatialPolygonStream.print();
				// Point-Polygon Range Query
//				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.SpatialRangeQuery(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep);
//				pointPolygonRangeQueryOutput.print();
				break;
			}
			case 403:{ // Range Query (GeoJSON LineString)
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
				DataStream<LineString> pointLineStringRangeQueryOutput = RangeQuery.LineStringRangeQuery(spatialLineStringStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				pointLineStringRangeQueryOutput.print();
				break;
			}
			case 501: { // Range Query (CSV Point)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.PointStream(geoJSONStream, "CSV", uGrid);
				spatialPointStream.print();
				DataStream<Point> rNeighbors= RangeQuery.PointRangeQuery(spatialPointStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);  // better than equivalent GB approach
				rNeighbors.print();
				break;}
			case 502:{ // Range Query (CSV Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(geoJSONStream, "CSV", uGrid);
				spatialPolygonStream.print();
				// Point-Polygon Range Query
				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				pointPolygonRangeQueryOutput.print();
				break;
			}
			case 503:{ // Range Query (CSV LineString)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.LineStringStream(geoJSONStream, "CSV", uGrid);
				spatialLineStringStream.print();
				break;
			}
			case 504:{ // GeometryCollection (CSV GeometryCollection no Timestamp)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<GeometryCollection> spatialLineStringStream = Deserialization.GeometryCollectionStream(geoJSONStream, "CSV", uGrid);
				spatialLineStringStream.print();
				break;
			}
			case 505:{ // MultiPoint (CSV MultiPoint no Timestamp)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<MultiPoint> spatialMultiPointStream = Deserialization.MultiPointStream(geoJSONStream, "CSV", uGrid);
				spatialMultiPointStream.print();
				break;
			}
			case 601: { // Range Query (TSV Point)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.PointStream(geoJSONStream, "TSV", uGrid);
				spatialPointStream.print();
				DataStream<Point> rNeighbors= RangeQuery.PointRangeQuery(spatialPointStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);  // better than equivalent GB approach
				rNeighbors.print();
				break;}
			case 602:{ // Range Query (TSV Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(geoJSONStream, "TSV", uGrid);
				spatialPolygonStream.print();
				// Point-Polygon Range Query
				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				pointPolygonRangeQueryOutput.print();
				break;
			}
			case 603:{ // Range Query (TSV LineString)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.LineStringStream(geoJSONStream, "TSV", uGrid);
				spatialLineStringStream.print();
				break;
			}
			case 604:{ // GeometryCollection (CSV GeometryCollection no Timestamp)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<GeometryCollection> spatialLineStringStream = Deserialization.GeometryCollectionStream(geoJSONStream, "TSV", uGrid);
				spatialLineStringStream.print();
				break;
			}
			case 605:{ // MultiPoint (CSV GeometryCollection no Timestamp)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<MultiPoint> spatialMultiPointStream = Deserialization.MultiPointStream(geoJSONStream, "TSV", uGrid);
				spatialMultiPointStream.print();
				break;
			}
			case 701:{ // TFilterQuery (GeoJSON Point Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(geoJSONStream, "GeoJSON", inputDateFormat, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PointToGeoJSONOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 702:{ // TFilterQuery (GeoJSON Polygon Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Polygon> spatialTrajectoryStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, "GeoJSON", inputDateFormat, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PolygonToGeoJSONOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 703:{ // TFilterQuery (GeoJSON LineString Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<LineString> spatialTrajectoryStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, "GeoJSON", inputDateFormat, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.LineStringToGeoJSONOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 704:{ // GeometryCollection (GeoJSON GeometryCollection Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<GeometryCollection> spatialTrajectoryStream = Deserialization.TrajectoryStreamGeometryCollection(geoJSONStream, "GeoJSON", inputDateFormat, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.GeometryCollectionToGeoJSONOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 705:{ // MultiPoint (GeoJSON MultiPoint Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<MultiPoint> spatialTrajectoryStream = Deserialization.TrajectoryStreamMultiPoint(geoJSONStream, "GeoJSON", inputDateFormat, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.MultiPointToGeoJSONOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 801:{ // TFilterQuery (CSV Point Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(geoJSONStream, "CSV", inputDateFormat, "timestamp", "oID", uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PointToCSVOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 802:{ // TFilterQuery (CSV Polygon Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Polygon> spatialTrajectoryStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, "CSV", inputDateFormat, null, null,uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PolygonToCSVOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 803:{ // TFilterQuery (CSV LineString Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<LineString> spatialTrajectoryStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, "CSV", inputDateFormat, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.LineStringToCSVOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 804:{ // GeometryCollection (CSV GeometryCollection Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<GeometryCollection> spatialTrajectoryStream = Deserialization.TrajectoryStreamGeometryCollection(geoJSONStream, "CSV", inputDateFormat, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.GeometryCollectionToCSVOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 805:{ // MultiPoint (CSV GeometryCollection Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<MultiPoint> spatialTrajectoryStream = Deserialization.TrajectoryStreamMultiPoint(geoJSONStream, "CSV", inputDateFormat, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.MultiPointToCSVOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 901:{ // TFilterQuery (TSV Point Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(geoJSONStream, "TSV", inputDateFormat, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PointToTSVOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 902:{ // TFilterQuery (TSV Polygon Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Polygon> spatialTrajectoryStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, "TSV", inputDateFormat, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PolygonToTSVOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 903:{ // TFilterQuery (TSV LineString Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<LineString> spatialTrajectoryStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, "TSV", inputDateFormat, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.LineStringToTSVOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 904:{ // GeometryCollection (TSV GeometryCollection Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<GeometryCollection> spatialTrajectoryStream = Deserialization.TrajectoryStreamGeometryCollection(geoJSONStream, "TSV", inputDateFormat, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.GeometryCollectionToTSVOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 905:{ // MultiPoint (TSV MultiPoint Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<MultiPoint> spatialTrajectoryStream = Deserialization.TrajectoryStreamMultiPoint(geoJSONStream, "TSV", inputDateFormat, null, null, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.MultiPointToTSVOutputSchema("kafka", inputDateFormat), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
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
				//TStatsQuery.TSpatialStatsQuery(ds, trajIDs).print();
				//TStatsQuery.TSpatialStatsQuery(ds, trajIDs, 5, 1).print();
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
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
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
				DataStream<Polygon> spatialStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, inputFormat, inputDateFormat, "timestamp", "oID", uGrid);
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
				//DataStream pointStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				//pointStream.print();
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(pointStream, inputFormat, inputDateFormat, ordinaryStreamAttributeNames.get(1), ordinaryStreamAttributeNames.get(0), uGrid);
				//spatialPointStream.print();
				Set<String> trajectoryIDSetPoint = new HashSet<String>();

				// Polygon stream
				DataStream polygonStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromLatest());
				//DataStream polygonStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				 //Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.TrajectoryStreamPolygon(polygonStream, inputFormat, inputDateFormat, queryStreamAttributeNames.get(1), queryStreamAttributeNames.get(0), uGrid);
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
				DataStream<Point> deimCheckInStream = Deserialization.TrajectoryStream(pointStream, inputFormat, inputDateFormat, ordinaryStreamAttributeNames.get(1), ordinaryStreamAttributeNames.get(0), uGrid);


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
