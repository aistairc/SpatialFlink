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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
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
		String inputTopicName;
		String outputTopicName;
		String bootStrapServers;
		int queryOption;
		double radius;
		int uniformGridSize;
		int windowSize;
		int windowSlideStep;
		int k;

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
			inputTopicName = "TaxiDriveGeoJSON_17M_R2_P60";
			outputTopicName = "outputTopicGeoFlink";

		}else{

			env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

			queryOption = 7;
			radius =  0.004;
			uniformGridSize = 200;
			windowSize = 5;
			windowSlideStep = 5;
			k = 10;
			bootStrapServers = "localhost:9092";
			//topicName = "TaxiDrive17MillionGeoJSON";
			inputTopicName = "NYCBuildingsPolygons";
			outputTopicName = "outputTopicGeoFlink";
		}


		/*
		// Boundaries for Taxi Drive dataset
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
		//DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>(topicName, new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
		//geoJSONStream.print();
		//DataStream csvStream  = env.addSource(new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), kafkaProperties).setStartFromEarliest());

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

		switch(queryOption) {

			case 1: { // Range Query (Grid-based)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("TaxiDrive17MillionGeoJSON", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = SpatialStream.PointStream(geoJSONStream, "GeoJSON", uGrid);
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(csvStream, "CSV", uGrid);
				DataStream<Point> rNeighbors= RangeQuery.SpatialRangeQuery(spatialPointStream, queryPoint, radius, windowSize, windowSlideStep, uGrid);  // better than equivalent GB approach
				rNeighbors.print();
				break;}
			case 2: { // KNN (Grid based - fixed radius)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("TaxiDrive17MillionGeoJSON", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = SpatialStream.PointStream(geoJSONStream, "GeoJSON", uGrid);
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(csvStream, "CSV", uGrid);
				DataStream < Tuple3<Long, Long, PriorityQueue<Tuple2<Point, Double>>>> kNNPQStream = KNNQuery.SpatialKNNQuery(spatialPointStream, queryPoint, radius, k, windowSize, windowSlideStep, uGrid);
				kNNPQStream.print();
				break;}
			case 3: { // KNN (Grid based - Iterative approach)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("TaxiDrive17MillionGeoJSON", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = SpatialStream.PointStream(geoJSONStream, "GeoJSON", uGrid);
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(csvStream, "CSV", uGrid);
				DataStream<PriorityQueue < Tuple2 < Point, Double >>> kNNPQStream = KNNQuery.SpatialIterativeKNNQuery(spatialPointStream, queryPoint, k, windowSize, windowSlideStep, uGrid);
				kNNPQStream.print();
				break;}
			case 4: { // Spatial Join (Grid-based)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("TaxiDrive17MillionGeoJSON", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to point spatial data stream
				DataStream<Point> spatialPointStream = SpatialStream.PointStream(geoJSONStream, "GeoJSON", uGrid);
				//DataStream<Point> spatialPointStream = SpatialStream.PointStream(csvStream, "CSV", uGrid);
				//Generating query stream
				DataStream geoJSONQueryStream  = env.addSource(new FlinkKafkaConsumer<>("TaxiDriveQueries1MillionGeoJSON_Live", new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream<Point> queryStream = SpatialStream.PointStream(geoJSONQueryStream, "GeoJSON", uGrid);
				DataStream<Tuple2<String, String>> spatialJoinStream = JoinQuery.SpatialJoinQuery(spatialPointStream, queryStream, radius, windowSize, windowSlideStep, uGrid);
				spatialJoinStream.print();
				break;}
			case 5:{ // Range Query (Point-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("NYCBuildingsPolygons", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = SpatialStream.PolygonStream(geoJSONStream, "GeoJSON", uGrid);
				// Point-Polygon Range Query
				DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.SpatialRangeQuery(spatialPolygonStream, queryPoint, radius, uGrid, windowSize, windowSlideStep);
				pointPolygonRangeQueryOutput.print();
				break;
			}
			case 6:{ // Range Query (Polygon-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("NYCBuildingsPolygons", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = SpatialStream.PolygonStream(geoJSONStream, "GeoJSON", uGrid);
				DataStream<Polygon> polygonPolygonRangeQueryOutput = RangeQuery.SpatialRangeQuery(spatialPolygonStream, queryPolygon, radius, uGrid, windowSize, windowSlideStep);
				polygonPolygonRangeQueryOutput.print();
				break;
			}
			case 7:{ // KNN Query (Point-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("NYCBuildingsPolygons", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = SpatialStream.PolygonStream(geoJSONStream, "GeoJSON", uGrid);
				// The output stream contains time-window boundaries (statring and ending time) and a Priority Queue containing topK query neighboring polygons
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> pointPolygonkNNQueryOutput = KNNQuery.SpatialKNNQuery(spatialPolygonStream, queryPoint, radius, k, uGrid, windowSize, windowSlideStep);
				pointPolygonkNNQueryOutput.print();
				break;
			}
			case 8:{ // KNN Query (Polygon-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("NYCBuildingsPolygons", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = SpatialStream.PolygonStream(geoJSONStream, "GeoJSON", uGrid);
				DataStream<Tuple3<Long, Long, PriorityQueue<Tuple2<Polygon, Double>>>> pointPolygonkNNQueryOutput = KNNQuery.SpatialKNNQuery(spatialPolygonStream, queryPolygon, radius, k, uGrid, windowSize, windowSlideStep);
				pointPolygonkNNQueryOutput.print();
				break;
			}
			case 9:{ // Join Query (Point-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("NYCBuildingsPolygons", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON,CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = SpatialStream.PolygonStream(geoJSONStream, "GeoJSON", uGrid);
				//spatialPolygonStream.print();

				//Generating query stream TaxiDrive17MillionGeoJSON
				//DataStream geoJSONQueryPointStream  = env.addSource(new FlinkKafkaConsumer<>("NYCFoursquareCheckIns", new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream geoJSONQueryPointStream  = env.addSource(new FlinkKafkaConsumer<>("NYCFourSquareCheckIns", new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromEarliest());
				DataStream<Point> queryPointStream = SpatialStream.PointStream(geoJSONQueryPointStream, "GeoJSON", uGrid);
				//geoJSONQueryPointStream.print();

				DataStream<Tuple2<String, String>> spatialJoinStream = JoinQuery.SpatialJoinQuery(spatialPolygonStream, queryPointStream, radius, uGrid, windowSize, windowSlideStep);
				spatialJoinStream.print();

				break;
			}
			case 10:{ // Join Query (Polygon-Polygon)
				DataStream geoJSONStream  = env.addSource(new FlinkKafkaConsumer<>("NYCBuildingsPolygons", new JSONKeyValueDeserializationSchema(false), kafkaProperties).setStartFromEarliest());
				// Converting GeoJSON, CSV stream to polygon spatial data stream
				DataStream<Polygon> spatialPolygonStream = SpatialStream.PolygonStream(geoJSONStream, "GeoJSON", uGrid);
				//spatialPolygonStream.print();

				//Generating query stream TaxiDrive17MillionGeoJSON
				//DataStream geoJSONQueryPolygonStream  = env.addSource(new FlinkKafkaConsumer<>("NYCFoursquareCheckIns", new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromLatest());
				DataStream geoJSONQueryPolygonStream  = env.addSource(new FlinkKafkaConsumer<>("NYCFourSquareCheckIns", new JSONKeyValueDeserializationSchema(false),kafkaProperties).setStartFromEarliest());
				DataStream<Polygon> queryPolygonStream = SpatialStream.PolygonStream(geoJSONQueryPolygonStream, "GeoJSON", uGrid);
				//queryPolygonStream.print();

				DataStream<Tuple2<String, String>> spatialJoinStream = JoinQuery.SpatialJoinQuery(spatialPolygonStream, queryPolygonStream,  windowSlideStep, windowSize, radius, uGrid);
				spatialJoinStream.print();
				break;
			}
			default:
				System.out.println("Input Unrecognized. Please select option from 1-10.");
		}


		// Execute program
		env.execute("Geo Flink");
	}
}