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
		int parallelism = params.parallelism;
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


		if (onCluster) {
			env = StreamExecutionEnvironment.getExecutionEnvironment();

		}else{
			env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
		}
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setParallelism(parallelism);

		/*
		// Boundaries for Taxi Drive dataset
		double minX = 115.50000;     //X - East-West longitude
		double maxX = 117.60000;
		double minY = 39.60000;     //Y - North-South latitude
		double maxY = 41.10000;
		 */

		// Preparing Kafka Connection to Get Stream Tuples
		Properties kafkaProperties = new Properties();
		//kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
		kafkaProperties.setProperty("bootstrap.servers", bootStrapServers);
		kafkaProperties.setProperty("group.id", "messageStream");

		// Defining Grid
		UniformGrid uGrid;
		UniformGrid qGrid;		

		// Dataset-specific Parameters		
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

		//polygonSet = queryPolygonSet;
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

		
		// Generating stream
		DataStream inputStream  = env.addSource(new FlinkKafkaConsumer<>(inputTopicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());

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
				//geoJSONStream.print();
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = Deserialization.TrajectoryStream(geoJSONStream, inputFormat, inputDateFormat, inputDelimiter1, csvTsvSchemaAttr1, "timestamp", "oID", uGrid);
				DataStream<Point> rNeighbors = new PointPointRangeQuery(realtimeConf, uGrid).run(spatialPointStream, qPointSet, radius);
				//rNeighbors.print();
				break;
			}
			default:
				System.out.println("Input Unrecognized. Please select option from 1-10.");
		}

		// Execute program
		env.execute("Geo Flink");
	}




}
