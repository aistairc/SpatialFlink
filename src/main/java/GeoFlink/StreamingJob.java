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
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialOperators.JoinQuery;
import GeoFlink.spatialOperators.KNNQuery;
import GeoFlink.spatialOperators.RangeQuery;
import GeoFlink.spatialStreams.SpatialStream;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import scala.Serializable;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;


public class StreamingJob implements Serializable {

	public static void main(String[] args) throws Exception {

		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		config.setString(RestOptions.BIND_PORT, "8081");

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);



		String test = "";
		String topicName = "sparkStream";
		String bootStrapServers = "localhost:9092";
		int queryOption = 1;
		double radius =  0.004;
		int uniformGridSize = 100;
		int windowSize = 10; // in seconds
		int windowSlideStep = 5; // in seconds
		int k = 3;

		// Boundaries for Taxi Drive dataset
		double minX = 115.50000;     //X - East-West longitude
		double maxX = 117.60000;
		double minY = 39.60000;     //Y - North-South latitude
		double maxY = 41.10000;

		// Defining Grid
		UniformGrid uGrid = new UniformGrid(uniformGridSize, minX, maxX, minY, maxY);

		// Preparing Kafka Connection to Get Stream Tuples
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", bootStrapServers);
		kafkaProperties.setProperty("group.id", "messageStream");
		DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(topicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
		//DataStream csvStream  = env.addSource(new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), kafkaProperties).setStartFromEarliest());

		// Converting GeoJSON,CSV stream to point spatial data stream
		DataStream<Point> spatialStream = SpatialStream.PointStream(geoJSONStream, "GeoJSON", uGrid);
		//DataStream<Point> spatialStream = SpatialStream.PointStream(csvStream, "CSV", uGrid);

		// Creating a query point
		Point queryPoint = new Point(116.414899, 39.920374, uGrid);

		switch(queryOption) {

			case 1: { // Range Query (Grid-based)
				DataStream<Point> rNeighbors= RangeQuery.SpatialRangeQuery(spatialStream, queryPoint, radius, windowSize, windowSlideStep, uGrid);  // better than equivalent GB approach
				rNeighbors.print();
				break;}
			case 2: { // KNN (Grid based - Iterative approach)
				DataStream < PriorityQueue < Tuple2 < Point, Double >>> kNNPQStream = KNNQuery.SpatialKNNQuery(spatialStream, queryPoint, k, windowSize, windowSlideStep, uGrid);
				kNNPQStream.print();
				break;}
			case 3: { // Spatial Join (Grid-based)
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>("TaxiDriveQueries1MillionGeoJSON_Live", new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = SpatialStream.PointStream(geoJSONQueryStream, "GeoJSON", uGrid);
				DataStream<Tuple2<String, String>> spatialJoinStream = JoinQuery.SpatialJoinQuery(spatialStream, queryStream, radius, windowSize, windowSlideStep, uGrid);
				spatialJoinStream.print();
				break;}
			default:
				System.out.println("Input Unrecognized. Please select an option from 1-3.");
		}

		// execute program
		env.execute("Geo Flink");
	}
}