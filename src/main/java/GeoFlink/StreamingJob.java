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

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.MultiLineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.spatialOperators.*;
import GeoFlink.spatialStreams.*;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
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

		//--onCluster "false" --dataset "TDriveBeijing" --queryOption "21" --inputTopicName "TaxiDrive17MillionGeoJSON" --outputTopicName "outputTopic" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd HH:mm:ss" --radius "0.005" --aggregate "SUM" --wType "TIME" --wInterval "10" --wStep "10" --uniformGridSize 25 --k "5" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1"
		//--onCluster "false" --dataset "TDriveBeijing" --queryOption "100" --inputTopicName "TaxiDrive17MillionGeoJSON" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd HH:mm:ss" --radius "0.0005" --aggregate "SUM" --wType "TIME" --wInterval "10" --wStep "10" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1"
		//--onCluster "false" --dataset "TDriveBeijing" --queryOption "21" --inputTopicName "TaxiDrive17MillionGeoJSON" --outputTopicName "outputTopic" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd HH:mm:ss" --radius "0.005" --aggregate "SUM" --wType "TIME" --wInterval "10" --wStep "10" --uniformGridSize 25 --k "5" --trajDeletionThreshold 1000  --gridMinX "115.50000" --gridMaxX "117.60000" --gridMinY "39.60000" --gridMaxY "41.10000" --trajIDSet "10007, 2560, 5261, 1743, 913" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.984416, 40.675882], [-73.984511, 40.675767], [-73.984719, 40.675867], [-73.984726, 40.67587], [-73.984718, 40.675881], [-73.984631, 40.675986], [-73.984416, 40.675882]" --queryLineString "[-73.984416, 40.675882], [-73.984511, 40.675767]" --outOfOrderAllowedLateness "1" --omegaJoinDuration "1"
		//--onCluster "false" --dataset "TDriveBeijing" --queryOption "100" --inputTopicName "TaxiDrive17MillionGeoJSON" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd HH:mm:ss" --radius "0.0005" --aggregate "SUM" --wType "TIME" --wInterval "10" --wStep "10" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000
		//--onCluster "false" --dataset "TDriveBeijing" --queryOption "100" --inputTopicName "DEIM2021" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd HH:mm:ss" --radius "0.0005" --aggregate "SUM" --wType "TIME" --wInterval "10" --wStep "10" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000
		//--onCluster "false" --approximateQuery "false" --queryOption "5" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "null" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.984416, 40.675882], [-73.984511, 40.675767], [-73.984719, 40.675867], [-73.984726, 40.67587], [-73.984718, 40.675881], [-73.984631, 40.675986], [-73.984416, 40.675882]" --queryLineString "[-73.984416, 40.675882], [-73.984511, 40.675767]"
		//--onCluster "false" --approximateQuery "false" --queryOption "1" --inputTopicName "TaxiDrive17MillionGeoJSON" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "yyyy-MM-dd HH:mm:ss" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "5" --wStep "3" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "115.50000" --gridMaxX "117.60000" --gridMinY "39.60000" --gridMaxY "41.10000" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[116.14319183444924, 440.07271444145411]" --queryPolygon "[-73.984416, 40.675882], [-73.984511, 40.675767], [-73.984719, 40.675867], [-73.984726, 40.67587], [-73.984718, 40.675881], [-73.984631, 40.675986], [-73.984416, 40.675882]" --queryLineString "[-73.984416, 40.675882], [-73.984511, 40.675767]"
		//--onCluster "false" --dataset "NYCBuildingsPolygons" --queryOption "5" --inputTopicName "NYCBuildingsPolygons" --queryTopicName "sampleTopic" --outputTopicName "QueryLatency" --inputFormat "GeoJSON" --dateFormat "null" --radius "0.05" --aggregate "SUM" --wType "TIME" --wInterval "1" --wStep "1" --uniformGridSize 100 --k "10" --trajDeletionThreshold 1000 --outOfOrderAllowedLateness "1" --omegaJoinDuration "1" --gridMinX "-74.25540" --gridMaxX "-73.70007" --gridMinY "40.49843" --gridMaxY "40.91506" --trajIDSet "9211800, 9320801, 9090500, 7282400, 10390100" --queryPoint "[-74.0000, 40.72714]" --queryPolygon "[-73.984416, 40.675882], [-73.984511, 40.675767], [-73.984719, 40.675867], [-73.984726, 40.67587], [-73.984718, 40.675881], [-73.984631, 40.675986], [-73.984416, 40.675882]" --queryLineString "[-73.984416, 40.675882], [-73.984511, 40.675767]"
		//TaxiDriveGeoJSON_Live


		ParameterTool parameters = ParameterTool.fromArgs(args);

		int queryOption = Integer.parseInt(parameters.get("queryOption"));
		String inputTopicName = parameters.get("inputTopicName");
		String queryTopicName = parameters.get("queryTopicName");
		String outputTopicName = parameters.get("outputTopicName");
		String inputFormat = parameters.get("inputFormat");
		String dateFormatStr = parameters.get("dateFormat");
		String aggregateFunction = parameters.get("aggregate");  // "ALL", "SUM", "AVG", "MIN", "MAX" (Default = ALL)
		double radius = Double.parseDouble(parameters.get("radius")); // Default 10x10 Grid
		int uniformGridSize = Integer.parseInt(parameters.get("uniformGridSize"));
		int windowSize = Integer.parseInt(parameters.get("wInterval"));
		int windowSlideStep = Integer.parseInt(parameters.get("wStep"));
		String windowType = parameters.get("wType");
		int k = Integer.parseInt(parameters.get("k")); // k denotes filter size in filter query
		boolean onCluster = Boolean.parseBoolean(parameters.get("onCluster"));
		boolean approximateQuery = Boolean.parseBoolean(parameters.get("approximateQuery"));
		//String dataset = parameters.get("dataset"); // TDriveBeijing, ATCShoppingMall
		Long inactiveTrajDeletionThreshold = Long.parseLong(parameters.get("trajDeletionThreshold"));
		int allowedLateness = Integer.parseInt(parameters.get("outOfOrderAllowedLateness"));
		int omegaJoinDurationSeconds = Integer.parseInt(parameters.get("omegaJoinDuration"));

		double gridMinX = Double.parseDouble(parameters.get("gridMinX"));
		double gridMaxX = Double.parseDouble(parameters.get("gridMaxX"));
		double gridMinY = Double.parseDouble(parameters.get("gridMinY"));
		double gridMaxY = Double.parseDouble(parameters.get("gridMaxY"));
		String trajIDSet = parameters.get("trajIDSet");
		List<Coordinate> queryPointCoordinates = getCoordinates(parameters.get("queryPoint"));
		List<Coordinate> queryLineStringCoordinates = getCoordinates(parameters.get("queryLineString"));
		List<Coordinate> queryPolygonCoordinates = getCoordinates(parameters.get("queryPolygon"));

		String bootStrapServers;
		DateFormat inputDateFormat;

		if(dateFormatStr.equals("null"))
			inputDateFormat = null;
		else
			inputDateFormat = new SimpleDateFormat(dateFormatStr);
			//inputDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // TDrive Dataset


		if (onCluster) {
			env = StreamExecutionEnvironment.getExecutionEnvironment();
			bootStrapServers = "172.16.0.64:9092, 172.16.0.81:9092";
		}else{
			env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
			bootStrapServers = "localhost:9092";
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
		env.setParallelism(10);

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
		//DataStream abc  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
		//abc.print();
		//DataStream csvStream  = env.addSource(new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), kafkaProperties).setStartFromEarliest());

		// Defining Grid
		UniformGrid uGrid;

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
		Polygon queryPoly;
		LineString queryLineString;

		uGrid = new UniformGrid(uniformGridSize, gridMinX, gridMaxX, gridMinY, gridMaxY);
		String[] trajID = trajIDSet.split("\\s*,\\s*");
		trajIDs = Stream.of(trajID).collect(Collectors.toSet());
		qPoint = new Point(queryPointCoordinates.get(0).x, queryPointCoordinates.get(0).y, uGrid);
		List<List<Coordinate>> listCoordinate = new ArrayList<List<Coordinate>>();
		listCoordinate.add(queryPolygonCoordinates);
		queryPoly = new Polygon(listCoordinate, uGrid);
		polygonSet = Stream.of(queryPoly).collect(Collectors.toSet());
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

			case 1: { // Range Query (Grid-based)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(geoJSONStream, inputFormat, uGrid);
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, uGrid);

				//DataStream<Point> rNeighbors = RangeQuery.PointRangeQueryIncremental(spatialPointStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				DataStream<Point> rNeighbors = RangeQuery.PointRangeQuery(spatialPointStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				//rNeighbors.print();

				//DataStream<Point> rNeighbors= RangeQuery.PointRangeQuery(spatialPointStream, qPoint, radius, uGrid, windowSize, windowSlideStep);  // better than equivalent GB approach
				//rNeighbors.print();
				break;}
			case 2: { // KNN (Grid based - fixed radius)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, uGrid);
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(geoJSONStream, inputFormat, uGrid);
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(csvStream, "CSV", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = KNNQuery.SpatialKNNQuery(spatialPointStream, qPoint, radius, k, windowSize, windowSlideStep, uGrid);
				kNNPQStream.print();
				break;}
			case 3: { // KNN (Grid based - Iterative approach)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, uGrid);
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(geoJSONStream, inputFormat, uGrid);
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(csvStream, "CSV", uGrid);
				DataStream<PriorityQueue < Tuple2 < Point, Double >>> kNNPQStream = KNNQuery.SpatialIterativeKNNQuery(spatialPointStream, qPoint, k, windowSize, windowSlideStep, uGrid);
				kNNPQStream.print();
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
				DataStream<Polygon> spatialPolygonStream = SpatialStream.PolygonStream(geoJSONStream, inputFormat, uGrid);
				DataStream<Polygon> polygonPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, queryPoly, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				polygonPolygonRangeQueryOutput.print();
				break;
			}
			case 7:{ // KNN Query (Point-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = SpatialStream.PolygonStream(geoJSONStream, inputFormat, uGrid);
				// The output stream contains time-window boundaries (starting and ending time) and a Priority Queue containing topK query neighboring polygons
				//DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> pointPolygonkNNQueryOutput = KNNQuery.SpatialKNNQuery(spatialPolygonStream, qPoint, radius, k, uGrid, windowSize, windowSlideStep);
				//pointPolygonkNNQueryOutput.print();
				break;
			}
			case 8:{ // KNN Query (Polygon-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = SpatialStream.PolygonStream(geoJSONStream, inputFormat, uGrid);
				//DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> pointPolygonkNNQueryOutput = KNNQuery.SpatialKNNQuery(spatialPolygonStream, queryPoly, radius, k, uGrid, windowSize, windowSlideStep);
				//pointPolygonkNNQueryOutput.print();
				break;
			}
			case 9:{ // Join Query (Point-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = SpatialStream.PolygonStream(geoJSONStream, inputFormat, uGrid);
				//spatialPolygonStream.print();

				//Generating query stream TaxiDrive17MillionGeoJSON
				//DataStream geoJSONQueryPointStream  = env.addSource(new FlinkKafkaConsumer<>("NYCFoursquareCheckIns", new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream geoJSONQueryPointStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromEarliest());
				DataStream<Point> queryPointStream = SpatialStream.PointStream(geoJSONQueryPointStream, "GeoJSON", uGrid);
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
				DataStream<Polygon> spatialPolygonStream = SpatialStream.PolygonStream(geoJSONStream, "GeoJSON", uGrid);
				//spatialPolygonStream.print();

				//Generating query stream TaxiDrive17MillionGeoJSON
				//DataStream geoJSONQueryPolygonStream  = env.addSource(new FlinkKafkaConsumer<>("NYCFoursquareCheckIns", new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream geoJSONQueryPolygonStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromEarliest());
				DataStream<Polygon> queryPolygonStream = SpatialStream.PolygonStream(geoJSONQueryPolygonStream, "GeoJSON", uGrid);
				//queryPolygonStream.print();

				//---Spatial Join using Neighboring Layers----
//				DataStream<Tuple2<String, String>> spatialJoinStream = JoinQuery.SpatialJoinQuery(spatialPolygonStream, queryPolygonStream,  windowSlideStep, windowSize, radius, uGrid);
//				spatialJoinStream.print();

				//----Optimized Spatial Join using Candidate and Guaranteed Neighbors---
//				DataStream<Tuple2<String, String>> spatialJoinStreamOptimized = JoinQuery.SpatialJoinQueryOptimized(spatialPolygonStream, queryPolygonStream,  windowSlideStep, windowSize, radius, uGrid);
//				spatialJoinStreamOptimized.print();
				break;
			}
			case 21:{ // TFilterQuery Real-time
				DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
				DataStream<Point> outputStream = TFilterQuery.TIDSpatialFilterQuery(spatialTrajectoryStream, trajIDs);
				//outputStream.print();
				outputStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkPoint(queryOption, outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 22:{ // TFilterQuery Windowed
				DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
				TFilterQuery.TIDSpatialFilterQuery(spatialTrajectoryStream, trajIDs, windowSize, windowSlideStep);
				break;
			}
			case 23:{ // TRangeQuery Real-time
				DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
				DataStream<Point> outputStream = TRangeQuery.TSpatialRangeQuery(spatialTrajectoryStream, polygonSet);
				//Naive
				//DataStream<Point> outputStream = TRangeQuery.TSpatialRangeQuery(polygonSet, spatialTrajectoryStream);

				//outputStream.print();
				outputStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkPoint(queryOption, outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 24:{ // TRangeQuery Windowed
				DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
				TRangeQuery.TSpatialRangeQuery(spatialTrajectoryStream, polygonSet, windowSize, windowSlideStep, allowedLateness);//.print();
				break;
			}
			case 25:{ // TStatsQuery Real-time
				DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
				DataStream<Tuple5<String, Double, Long, Double, Long>> outputStream = TStatsQuery.TSpatialStatsQuery(spatialTrajectoryStream, trajIDs);
				//outputStream.print();
				outputStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkTuple5(queryOption, outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 26:{ // TStatsQuery Windowed
				DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
				TStatsQuery.TSpatialStatsQuery(spatialTrajectoryStream, trajIDs, windowSize, windowSlideStep);
				break;
			}
			case 27:{ // TSpatialHeatmapAggregateQuery Real-time
				DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
				DataStream<Tuple4<String, Integer, HashMap<String, Long>, Long>> outputStream = TAggregateQuery.TSpatialHeatmapAggregateQuery(spatialTrajectoryStream, aggregateFunction, inactiveTrajDeletionThreshold);
				//outputStream.print();
				outputStream.addSink(new FlinkKafkaProducer<>(outputTopicName, new HelperClass.LatencySinkTuple4(queryOption, outputTopicName), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 28:{ // TAggregateQuery Windowed
				DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
				TAggregateQuery.TSpatialHeatmapAggregateQuery(spatialTrajectoryStream, aggregateFunction, windowType, windowSize, windowSlideStep);
				break;
			}
			case 29:{ // TSpatialJoinQuery Real-time
				// Generating query stream
				DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
				DataStream queryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromLatest());
				DataStream<Point> spatialQueryStream = SpatialStream.TrajectoryStream(queryStream, inputFormat, inputDateFormat, uGrid);
				TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, spatialQueryStream, radius, omegaJoinDurationSeconds, allowedLateness, uGrid);
				//TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, spatialQueryStream, radius, windowSize, windowSlideStep, allowedLateness, uGrid).print();

				// Self Stream Join
				//TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, radius, omegaJoinDurationSeconds, allowedLateness, uGrid);//.print();

				// Naive
				//TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, spatialQueryStream, radius, windowSize);

				break;
			}
			case 30:{ // TSpatialJoinQuery Windowed
				// Generating query stream
				DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
				DataStream queryStream  = env.addSource(new FlinkKafkaConsumer<>(queryTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromLatest());
				DataStream<Point> spatialQueryStream = SpatialStream.TrajectoryStream(queryStream, inputFormat, inputDateFormat, uGrid);
				TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, spatialQueryStream, radius, windowSize, windowSlideStep, allowedLateness, uGrid);//.print();

				// Self Stream Join
				TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, radius, omegaJoinDurationSeconds, allowedLateness, uGrid);//.print();

				// Naive
				//TJoinQuery.TSpatialJoinQuery(spatialTrajectoryStream, spatialQueryStream, radius, windowSize);

				break;
			}
			case 31:{ // TSpatialKNNQuery Real-time
				//System.out.println("qPoint " + qPoint);
				DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
				//Real-time kNN
				TKNNQuery.TSpatialKNNQuery(spatialTrajectoryStream, qPoint, radius, k, omegaJoinDurationSeconds, allowedLateness, uGrid); //.print();
				// Naive
				//TKNNQuery.TSpatialKNNQuery(spatialTrajectoryStream, qPoint, radius, k, windowSize, windowSlideStep, allowedLateness);

				break;
			}
			case 32:{ // TSpatialKNNQuery Windowed
				DataStream<Point> spatialTrajectoryStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);
				//Window-based kNN
				TKNNQuery.TSpatialKNNQuery(spatialTrajectoryStream, qPoint, radius, k, windowSize, windowSlideStep, allowedLateness, uGrid);

				break;
			}
			case 41: { // Range Query (GeoJSON Point)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.PointStream(geoJSONStream, "GeoJSON", uGrid);
				spatialPointStream.print();
//				DataStream<Point> rNeighbors= RangeQuery.SpatialRangeQuery(spatialPointStream, qPoint, radius, windowSize, windowSlideStep, uGrid);  // better than equivalent GB approach
//				rNeighbors.print();

				break;}
			case 42:{ // Range Query (GeoJSON Polygon)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(geoJSONStream, "GeoJSON", uGrid);
				spatialPolygonStream.print();
				// Point-Polygon Range Query
//				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.SpatialRangeQuery(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep);
//				pointPolygonRangeQueryOutput.print();
				break;
			}
			case 43:{ // Range Query (GeoJSON LineString)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.LineStringStream(geoJSONStream, "GeoJSON", uGrid);
				spatialLineStringStream.print();
				break;
			}
			case 45:{ // Range Query (Point-LineString)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to linestring spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.LineStringStream(geoJSONStream, "GeoJSON", uGrid);
				spatialLineStringStream.print();
				// Point-Polygon Range Query
				DataStream<LineString> pointLineStringRangeQueryOutput = RangeQuery.LineStringRangeQuery(spatialLineStringStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				pointLineStringRangeQueryOutput.print();
				break;
			}
			case 51: { // Range Query (CSV Point)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.PointStream(geoJSONStream, "CSV", uGrid);
				spatialPointStream.print();
				DataStream<Point> rNeighbors= RangeQuery.PointRangeQuery(spatialPointStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);  // better than equivalent GB approach
				rNeighbors.print();
				break;}
			case 52:{ // Range Query (CSV Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(geoJSONStream, "CSV", uGrid);
				spatialPolygonStream.print();
				// Point-Polygon Range Query
				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				pointPolygonRangeQueryOutput.print();
				break;
			}
			case 53:{ // Range Query (CSV LineString)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.LineStringStream(geoJSONStream, "CSV", uGrid);
				spatialLineStringStream.print();
				break;
			}
			case 61: { // Range Query (TSV Point)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.PointStream(geoJSONStream, "TSV", uGrid);
				spatialPointStream.print();
				DataStream<Point> rNeighbors= RangeQuery.PointRangeQuery(spatialPointStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);  // better than equivalent GB approach
				rNeighbors.print();
				break;}
			case 62:{ // Range Query (TSV Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = Deserialization.PolygonStream(geoJSONStream, "TSV", uGrid);
				spatialPolygonStream.print();
				// Point-Polygon Range Query
				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.PolygonRangeQuery(spatialPolygonStream, qPoint, radius, uGrid, windowSize, windowSlideStep, allowedLateness, approximateQuery);
				pointPolygonRangeQueryOutput.print();
				break;
			}
			case 63:{ // Range Query (TSV LineString)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<LineString> spatialLineStringStream = Deserialization.LineStringStream(geoJSONStream, "TSV", uGrid);
				spatialLineStringStream.print();
				break;
			}

			case 71:{ // TFilterQuery (GeoJSON Point Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(geoJSONStream, "GeoJSON", inputDateFormat, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PointToGeoJSONOutputSchema("kafka"), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 72:{ // TFilterQuery (GeoJSON Polygon Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Polygon> spatialTrajectoryStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, "GeoJSON", inputDateFormat, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PolygonToGeoJSONOutputSchema("kafka"), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 73:{ // TFilterQuery (GeoJSON LineString Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<LineString> spatialTrajectoryStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, "GeoJSON", inputDateFormat, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.LineStringToGeoJSONOutputSchema("kafka"), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 81:{ // TFilterQuery (CSV Point Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(geoJSONStream, "CSV", inputDateFormat, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PointToCSVOutputSchema("kafka"), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 82:{ // TFilterQuery (CSV Polygon Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Polygon> spatialTrajectoryStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, "CSV", inputDateFormat, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PolygonToCSVOutputSchema("kafka"), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 83:{ // TFilterQuery (CSV LineString Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<LineString> spatialTrajectoryStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, "CSV", inputDateFormat, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.LineStringToCSVOutputSchema("kafka"), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 91:{ // TFilterQuery (TSV Point Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Point> spatialTrajectoryStream = Deserialization.TrajectoryStream(geoJSONStream, "TSV", inputDateFormat, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PointToTSVOutputSchema("kafka"), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 92:{ // TFilterQuery (TSV Polygon Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<Polygon> spatialTrajectoryStream = Deserialization.TrajectoryStreamPolygon(geoJSONStream, "TSV", inputDateFormat, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.PolygonToTSVOutputSchema("kafka"), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 93:{ // TFilterQuery (TSV LineString Timestamp)
				DataStream geoJSONStream = env.addSource(new FlinkKafkaConsumer<>("kafka", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				DataStream<LineString> spatialTrajectoryStream = Deserialization.TrajectoryStreamLineString(geoJSONStream, "TSV", inputDateFormat, uGrid);
				spatialTrajectoryStream.print();
				spatialTrajectoryStream.addSink(new FlinkKafkaProducer<>("kafka", new Serialization.LineStringToTSVOutputSchema("kafka"), kafkaProperties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
				break;
			}
			case 101:{ // Shapefile (Point)
				Path path = new Path("D:\\testData\\point\\W01-05_GML\\W01-05-g_Dam.shp");
				ShapeFileInputFormat<Polygon> shapeFileInputFormat = new ShapeFileInputFormat<Polygon>(path, uGrid);
				TypeInformation<Polygon> typeInfo = TypeInformation.of(Polygon.class);
				DataStream<Polygon> stream =  env.createInput(shapeFileInputFormat, typeInfo);
				stream.print();
				break;
			}
			case 102:{ // Shapefile (Polygon)
				Path path = new Path("D:\\testData\\polygon\\japan_ver821.shp");
				ShapeFileInputFormat<Polygon> shapeFileInputFormat = new ShapeFileInputFormat<Polygon>(path, uGrid);
				TypeInformation<Polygon> typeInfo = TypeInformation.of(Polygon.class);
				DataStream<Polygon> stream =  env.createInput(shapeFileInputFormat, typeInfo);
				stream.print();
				break;
			}
			case 103:{ // Shapefile (LineString)
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
			case 100:{
				HashMap<String, Integer> roomCapacities = new HashMap<>();
				roomCapacities.put("A", 100);
				roomCapacities.put("B", 100);
				roomCapacities.put("C", 100);

				//inputDateFormat = "12/25/2020 17:24:36 +0900";
				inputDateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss z"); // TDrive Dataset
				inputFormat = "JSON";
				DataStream<Point> deimCheckInStream = SpatialStream.TrajectoryStream(inputStream, inputFormat, inputDateFormat, uGrid);

				//CheckIn.CheckInQuery(deimCheckInStream, roomCapacities, 24).print();
			}
			default:
				System.out.println("Input Unrecognized. Please select option from 1-10.");
		}

		// Execute program
		env.execute("Geo Flink");
	}

	private static List<Coordinate> getCoordinates(String target) {
		//"[100.0, 0.0], [103.0, 0.0], [103.0, 1.0], [102.0, 1.0], [100.0, 0.0]"
		List<Coordinate> list = new ArrayList<Coordinate>();
		if (target == null) {
			return list;
		}
		Pattern pattern = Pattern.compile("\\[(.+?)\\]");
		Matcher matcher = pattern.matcher(target);
		while (matcher.find()) {
			try {
				String[] arr = matcher.group(1).trim().split("\\s*,\\s*");
				list.add(new Coordinate(Double.valueOf(arr[0]), Double.valueOf(arr[1])));
			}
			catch (Exception e) {}
		}
		return list;
	}
}

