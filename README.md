# GeoFlink: A Scalable Framework for the Real-Time Processing of Spatial Data Streams

GeoFlink is an extension of Apache Flink --  a scalable opensource distributed streaming engine -- for the real-time processing of unbounded spatial streams. GeoFlink leverages a grid-based index for preserving spatial data proximity and pruning of objects which cannot be part of a spatial query result. Thus, providing effective data distribution that guarantees reduced query processing time.

GeoFlink supports spatial range, spatial *k*NN and spatial join queries. Details of GeoFlink architecture and experimental study demonstrating GeoFlink achieving higher query performance than other ordinary distributed approaches is available here:

[https://arxiv.org/abs/2004.03352v1](https://arxiv.org/abs/2004.03352v1) 

# Spatial Streaming in GeoFlink

GeoFlink currently supports CSV and GeoJSON input formats from Apache Kafka and Point spatial object. Future releases will extend support to other input formats and spatial object types including line and polygon.

All queries illustrated in this section make use of aggregation windows and are continuous in nature, i.e., they generate window-based continuous results on the continuous data stream. Namely, one output is generated per window aggregation.

In the following code snippets, longitude is referred as X and latitude as Y. 

## Defining Spatial Boundaries & Creating a Grid Index
Before running queries on GeoFlink, a Grid index needs to be defined. These are the spatial bounds where all of the spatial objects and query points are expected to lie.  This step generates a grid index, which forms the backbone of GeoFlink's optimized query processing. 

The Grid index is constructed by partitioning the 2D space given by its boundary *(MinX, MinY), (MaxX, MaxY)*  *(MaxX-MinX = MaxY-MinY)* into square shaped cells of length *l*. Smaller *l* results in the finer data distribution and pruning. However, very small *l* increases number of cells exponentially incurring higher processing costs and lowering throughput. 

In the following example, the grid spans over Beijing, China. 
```
/Defining dataStream boundaries & creating index
double minX = 115.50, maxX = 117.60, minY = 39.60, maxY = 41.10;
int gridSize = 100;
UniformGrid  uGrid = new UniformGrid(gridSize, minX, maxX, minY, maxY);
```
where *GridSize* of 100 generates a grid index of 100x100 cells, with the bottom-left (minX, minY) and top-right (maxX, maxY) coordinates, respectively.

## Defining a Spatial Data Stream
GeoFlink users need to make an appropriate Apache Kafka connection by specifying the topic name and bootstrap server(s). Once the connection is established, the user can construct spatial stream using the GeoFlink Java/Scala API. 

Currently, GeoFlink supports only 'point' type objects. A Point class is a GeoFlink class that initializes incoming spatial data by creating it's Point object using x,y coordinates.
```
//Kafka GeoJSON stream to Spatial points stream
DataStream<Point> spatialStream = SpatialStream.PointStream(kafkaStream, "GeoJSON", uGrid);
```
where *uGrid* is the grid index.

## Continuous Range Query
Given a data stream *S*, query point *q*, radius *r* and window parameters, range query returns the *r*-neighbours of *q* in *S* for each aggregation window.

To execute a spatial range query via GeoFlink, Java/Scala API  *SpatialRangeQuery* method of the *RangeQuery* class is used.

```
// Query point creation
Point queryPoint = new Point(116.414899, 39.920374, uGrid);

// Continous range query 
int windowSize = 10; // window size in seconds
int windowSlideStep = 5; // window slide step in seconds
int queryRadius = 0.5;

DataStream<Point> rNeighborsStream = RangeQuery.SpatialRangeQuery(spatialStream, queryPoint, queryRadius, windowSize, windowSlideStep, uGrid); 
```
where *spatialStream* is a spatial data stream, *queryPoint* denotes a query point, *radius* denotes the range query radius, *windowSize* and *windowSlideStep* denote the sliding window size and slide step respectively and *uGrid* denotes the grid index. The query output is generated continuously for each window slide based on size and slide step.

## Continuous *k*NN Query

Given a data stream *S*, a query point *q*, a positive integer *k* and window parameters, *k*NN query returns *k* nearest neighbours of *q* in *S* for each slide of the aggregation window.

To execute a spatial *k*NN query in GeoFlink, *SpatialKNNQuery* method of the *KNNQuery* class is used.
```
// Query point creation
Point queryPoint = new Point(116.414899, 39.920374, uGrid);

// Define input parameters
k = 50
int windowSize = 10; // window size in seconds
int windowSlideStep = 5; // window slide step in seconds
int queryRadius = 0.5;

// Register a spatial *k*NN query
DataStream <PriorityQueue<Tuple2<Point, Double>>> outputStream = KNNQuery.SpatialKNNQuery(spatialStream, queryPoint, k, windowSize, windowSlideStep, uGrid);
```

where *k* denotes the number of points required in the *k*NN output, *spatialStream* denotes a spatial data stream, *queryPoint* denotes a query point, *windowSize* and *windowSlideStep* denote the sliding window size and slide step respectively and *uGrid* denotes the grid index. 


 Please note that the output stream is a stream of priority queue which is a sorted list of *k*NNs with respect to the distance from the query point *q*. The query output is generated continuously for each window slide based on the *windowSize* and *windowSlideStep*.



## Continuous Join Query
Given two streams *S1* (Ordinary stream) and *S2* (Query stream), a radius *r* and window parameters, spatial join query returns all the points in *S1* that lie within the radius *r* of *S2* points for each aggregation window.

To execute a spatial join query via the GeoFlink Java/Scala API  *SpatialJoinQuery* method of the *JoinQuery* class is used. The query output is generated continuously for each window slide based on the *windowSize* and *windowSlideStep*.

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
where *spatialStream* and *queryStream* denote the ordinary stream and query stream, respectively and *uGrid* denotes the grid index. 
 The query output is generated continuously for each window slide based on the *windowSize* and *windowSlideStep*.




## Sample GeoFlink Code for a Spatial Range Query


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
