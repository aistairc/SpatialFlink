# GeoFlink: A Scalable Framework for the Real-Time Processing of Spatial Data Streams

GeoFlink is a Java and Scala based extension of Apache Flink --  a scalable opensource distributed streaming engine -- for the real-time processing of bounded and unbounded spatial streams. GeoFlink leverages a grid-based index for preserving spatial data proximity and pruning of objects which cannot be part of a spatial query result thus providing effective data distribution that guarantees reduced query processing time.

GeoFlink supports spatial range, spatial *k*NN and spatial join queries. Details of GeoFlink architecture and experimental study demonstrating GeoFlink achieving higher query performance than other ordinary distributed approaches on real spatial data streams is available here:

[https://arxiv.org/abs/2004.03352v1](https://arxiv.org/abs/2004.03352v1) 

# Spatial Streaming in GeoFlink

GeoFlink currently supports CSV and GeoJSON input formats from Apache Kafka and Point type spatial object. Future releases will extend support to other input formats and spatial object types including line and polygon.

All queries illustrated in this section make use of aggregation windows and are continuous in nature, i.e., they generate window-based continuous results on the continuous data stream. Namely, one output is generated per window aggregation.

In the following code snippets, longitude is referred as X and latitude as Y. 

## Defining Spatial Boundaries & Creating a Grid Index
Before running queries,a query space needs to be defined. These are the spatial bounds where majority of spatial objects or queries for a use case are expected to lie within.  This step generates an appropriate grid index for that region which forms the backbone of GeoFlink's optimized query processing. 

Points lying outside the rectangular grid space are still evaluated **(are they??)** however performance will suffer with an increase in these anomalies. Hence, the importance of selecting a good query space. 

A user must set the apporximate minimum and and maximum X *(longitude)* and Y *(latitude)* coordinates that span their query space an an appropriate grid size *(n)*. The higher the grid size, the finer the data distribution and pruning. However, very high grid sizes can increase number of cells exponentially incurring higher processing costs and lowering throughput. 

In the following example, the grid space spans over Beijing, China. 
```
/Defining dataStream boundaries & creating index
double minX = 115.50, maxX = 117.60, minY = 39.60, maxY = 41.10;
int gridSize = 100;
UniformGrid  uGrid = new UniformGrid(gridSize, minX, maxX, minY, maxY);
```

## Defining a Spatial Data Stream
GeoFlink users need to make an appropriate Apache Kafka connection by specifying the topic name and bootstrap server(s). Once the connection is established, the user can construct spatial stream using the GeoFlink Java/Scala API. 

Currently, GeoFlink only supports 'point' type objects in spatial streaming. A Point class is a GeoFlink class that initializes incoming spatial data by creating it's Point object using x,y coordinates **(and object ID?)**. GeoFlink assigns an immutable Grid ID to point objects in the stream upon their initialization.

```
//Kafka GeoJSON stream to Spatial points stream
DataStream<Point> spatialStream = SpatialStream.PointStream(kafkaStream, "GeoJSON", uGrid);
```
## Continuous Range Query
The spatial range query returns all spatial objects in a spatial stream, that lie within a user defined cut-off radius of the query point. The query results are generated periodically, based on the window size.

Parameters that need to be defined are a query point, query radius, window size and slide-step in order to query a continuous spatial stream.

```
// Query point creation
Point queryPoint = new Point(116.414899, 39.920374, uGrid);

// Continous range query 
int windowSize = 10; // window size in seconds
int windowSlideStep = 5; // window slide step in seconds
int queryRadius = 0.5;

DataStream<Point> rNeighborsStream = RangeQuery.SpatialRangeQuery(spatialStream, queryPoint, radius, windowSize, windowSlideStep, uGrid); 
```
The query output is generated continuously for each window slide.

## Continuous *k*NN Query

The spatial *k*NN query returns window-based *k*-nearest neighbors of a query point in the spatial stream. The query ranks the distances between the spatial objects and the query point and returns the *k* nearest neighbors periodically based on window size.

Parameters that need to be defined are a query point, the number of *k* nearest neighbors, query radius, window size and slide-step in order to query a continuous spatial stream.

```
// Query point creation
Point queryPoint = new Point(116.414899, 39.920374, uGrid);

// Define input parameters
k = 50
int windowSize = 10; // window size in seconds
int windowSlideStep = 5; // window slide step in seconds
int queryRadius = 0.5;

// Register a spatial $k$NN query
DataStream <PriorityQueue<Tuple2<Point, Double>>> outputStream = KNNQuery.SpatialKNNQuery(spatialStream, queryPoint, k, windowSize, windowSlideStep, uGrid);
```
Please note that the output is a stream of priority queue which is a sorted list of *k*NNs with respect to the distance from the query point. The query output is generated continuously for each window slide.

## Continuous Join Query
A spatial join query returns all the points in a spatial stream that lie within a user-defined cut-off radius of points in another query stream for each aggregation window.

Parameters that need to be defined are a query stream, query radius, window size and slide-step in order to query a continuous spatial stream.

```
// Create a query stream
DataStream<Point> queryStream = SpatialStream.
PointStream(geoJSONQryStream,"GeoJSON",uGrid);

// Define input parameters
int queryRadius = 0.5;
int windowSize = 10; // window size in seconds
int windowSlideStep = 5; // window slide step in seconds

// Register a spatial join query
DataStream<Tuple2<String,String>>outputStream = JoinQuery.SpatialJoinQuery(spatialStream, queryStream, queryRadius, windowSize, windowSlideStep, uGrid);
```
The output is a two-tuple data stream that contains the object ID of a point in query stream and a list of object IDs from the spatial stream, that lie within r-distance of it. **(???)** The query output is generated continuously for each window slide.
































### -----------EOF Reached-------------------------Previous README.md TO BE REMOVED---------

GeoFlink extends Apache Flink to support spatial data types, index and continuous queries over data streams. To enable the efficient processing of continuous spatial queries and for the effective data distribution among the Flink cluster nodes, a gird-based index is used. GeoFlink currently supports spatial range, spatial kNN and spatial join queries on point data type.

A sample GeoFlink code written in Java to execute Range Query on a spatial data stream

    //Defining dataStream boundaries & creating index
    double minX = 115.50, maxX = 117.60, minY = 39.60, maxY = 41.10;
    int gridSize = 100;
    UniformGrid  uGrid = new UniformGrid(gridSize, minX, maxX, minY, maxY);
    
    //Kafka GeoJSON stream to Spatial points stream
    DataStream<Point> spatialStream = SpatialStream.PointStream(kafkaStream, "GeoJSON", uGrid);
    
    //Query point creation
    Point queryPoint = new Point(116.414899, 39.920374, uGrid);
    
    //Continous range query 
    int windowSize = 10; // window size in seconds
    int windowSlideStep = 5; // window slide step in seconds
    int queryRadius = 0.5;
    DataStream<Point> rNeighborsStream = RangeQuery.SpatialRangeQuery(spatialStream, queryPoint, radius, windowSize, windowSlideStep, uGrid); 
