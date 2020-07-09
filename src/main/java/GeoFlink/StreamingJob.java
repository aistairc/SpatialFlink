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
import GeoFlink.spatialObjects.Polygon;
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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.locationtech.jts.geom.Coordinate;
import scala.Serializable;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;
import org.json.*;


public class StreamingJob implements Serializable {

	public static void main(String[] args) throws Exception {

		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		config.setString(RestOptions.BIND_PORT, "8081");  // Can be commented if default port is available

		// Set up the streaming execution environment
		final StreamExecutionEnvironment env;

		//INPUT FORMAT FOR CLUSTER: <True> <Query_number> <Radius> <Grid_size> <Window_size> <Slide_step>
		if(args.length < 1)
		{
			System.out.println("At-leaset one argument must be provided. |true| for cluster and |false| for local processing");
			System.exit(0);
		}

		boolean onCluster = Boolean.parseBoolean(args[0]);
		String topicName;
		String bootStrapServers;
		int queryOption = 3;
		double radius = 0;
		int uniformGridSize = 100;
		int windowSlideStep = 0;
		int windowSize = 0;
		int k = 3; // default value

		if (onCluster) {

			if(args.length < 7)
			{
				System.out.println("Input argument if onCluster (true/false) and the query option.");
				System.out.println("INPUT FORMAT FOR CLUSTER: <True> <Query_number> <Radius> <Grid_size> <Window_size> <Slide_step> <k>, E.g.: True 1 0.01 100 1 1 3");
				System.exit(0);
			}

			env = StreamExecutionEnvironment.getExecutionEnvironment();

			queryOption =  Integer.parseInt(args[1]);
			radius =  Double.parseDouble(args[2]);
			uniformGridSize = Integer.parseInt(args[3]);
			windowSize = Integer.parseInt(args[4]);
			windowSlideStep = Integer.parseInt(args[5]);
			k = Integer.parseInt(args[6]);
			bootStrapServers = "172.16.0.64:9092, 172.16.0.81:9092";
			topicName = "TaxiDriveGeoJSON_17M_R2_P60";

		}else{

			env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

			queryOption = 2;
			radius =  0.004;
			uniformGridSize = 500;
			windowSize = 10;
			windowSlideStep = 10;
			k = 3;
			bootStrapServers = "localhost:9092";
			//topicName = "TaxiDrive17MillionGeoJSON";
			topicName = "NYCBuildingsPolygons";
		}

		// Boundaries for Taxi Drive dataset
		/*
		double minX = 115.50000;     //X - East-West longitude
		double maxX = 117.60000;
		double minY = 39.60000;     //Y - North-South latitude
		double maxY = 41.10000;
		*/


		// Boundaries for NYC dataset
		double minX = -74.25540;     //X - East-West longitude
		double maxX = -73.70007;
		double minY = 40.49843;     //Y - North-South latitude
		double maxY = 40.91506;

		// Defining Grid
		UniformGrid uGrid = new UniformGrid(uniformGridSize, minX, maxX, minY, maxY);

		// Preparing Kafka Connection to Get Stream Tuples
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", bootStrapServers);
		kafkaProperties.setProperty("group.id", "messageStream");
		DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(topicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
		//geoJSONStream.print();
		//DataStream csvStream  = env.addSource(new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), kafkaProperties).setStartFromEarliest());

		// Converting GeoJSON,CSV stream to point spatial data stream
		//DataStream<Point> spatialStream = SpatialStream.PointStream(geoJSONStream, "GeoJSON", uGrid);
		//DataStream<Point> spatialStream = SpatialStream.PointStream(csvStream, "CSV", uGrid);
		DataStream<Polygon> spatialStream = SpatialStream.PolygonStream(geoJSONStream, "GeoJSON", uGrid);

		Point queryPoint = new Point(-73.9857, 40.6789, uGrid);

		// Point-Polygon Range Query
		//DataStream<Polygon> polygonStream = RangeQuery.SpatialRangeQuery(spatialStream, queryPoint, radius, uGrid, windowSize, windowSlideStep);


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
		DataStream<Polygon> polygonStream = RangeQuery.SpatialRangeQuery(spatialStream, queryPolygon, radius, uGrid, windowSize, windowSlideStep);
		polygonStream.print();

		//Point queryPoint = new Point(116.414899, 39.920374, uGrid);
		switch(queryOption) {

			case 1: { // Range Query (Grid-based)
				//DataStream<Point> rNeighbors= RangeQuery.SpatialRangeQuery(spatialStream, queryPoint, radius, windowSize, windowSlideStep, uGrid);  // better than equivalent GB approach
				//rNeighbors.print();
				break;}
			case 2: { // KNN (Grid based - fixed radius)
				//DataStream < PriorityQueue < Tuple2 < Point, Double >>> kNNPQStream = KNNQuery.SpatialKNNQuery(spatialStream, queryPoint, radius, k, windowSize, windowSlideStep, uGrid);
				//kNNPQStream.print();
				break;}
			case 3: { // KNN (Grid based - Iterative approach)
				//DataStream < PriorityQueue < Tuple2 < Point, Double >>> kNNPQStream = KNNQuery.SpatialIterativeKNNQuery(spatialStream, queryPoint, k, windowSize, windowSlideStep, uGrid);
				//kNNPQStream.print();
				break;}
			case 4: { // Spatial Join (Grid-based)
				//DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>("TaxiDriveQueries1MillionGeoJSON_Live", new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				//DataStream<Point> queryStream = SpatialStream.PointStream(geoJSONQueryStream, "GeoJSON", uGrid);
				//DataStream<Tuple2<String, String>> spatialJoinStream = JoinQuery.SpatialJoinQuery(spatialStream, queryStream, radius, windowSize, windowSlideStep, uGrid);
				//spatialJoinStream.print();
				break;}
			default:
				System.out.println("Input Unrecognized. Please select option from 1-3.");
		}

		// Execute program
		env.execute("Geo Flink");
	}
}