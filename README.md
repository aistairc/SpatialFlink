# GeoFlink: A Distributed Framework for the Real-Time Processing of Spatial Data Streams

## Table of Contents

 1. [ Introduction ](#intro)
 2. [ Spatial Stream Processing via GeoFlink ](#streamProcessing)
    1. [ Creating a Grid Index ](#gridIndex)
    2. [ Defining a Spatial Data Stream ](#defineSpatialStream)
    3. [ Continuous Spatial Range Query ](#range)
	    1. [Point-point](#pointPointRange) 
	    2. [Point-polygon](#pointPolygonRange) 
	    3. [ Polygon-polygon ](#polygonPolygonRange)
    4. [ Continuous Spatial *k*NN Query ](#kNN)
	   <!--  1. [Fixed Radius *k*NN](#FkNN)  
	    2. [Iterative *k*NN](#IkNN) -->
    5. [ Continuous Spatial Join Query ](#join)
3. [ Getting Started ](#gettingStarted)
    1. [ Requirements ](#requirements)
    2. [ Running Your First GeoFlink Job ](#firstJob)
4. [ Sample GeoFlink Code for a Spatial Range Query ](#sampleCode)
5. [ Publications ](#publications)
6. [ Contact Us! ](#contact)


<a name="intro"></a>
## Introduction
GeoFlink is an extension of Apache Flink — a scalable opensource distributed streaming engine — for the real-time processing of unbounded spatial streams. GeoFlink leverages a grid-based index for preserving spatial data proximity and pruning of objects which cannot be part of a spatial query result. Thus, providing effective data distribution that guarantees reduced query processing time.

GeoFlink supports spatial range, spatial *k*NN and spatial join queries. Please refer to the [ Publications ](#publications) section for details of the architecture and experimental study demonstrating GeoFlink achieving higher query performance than other ordinary distributed approaches.

<a name="streamProcessing"></a>
## Spatial Stream Processing via GeoFlink

GeoFlink currently supports GeoJSON and CSV input formats from Apache Kafka and Point spatial object. Future releases will extend support to other input formats and spatial object types including line and polygon.

GeoJSON is a format for encoding a variety of geographic data structures. Its basic element consists of the *type*, *geometry* and *properties* members. The geometry member contains its type (`Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, and `MultiPolygon`) and coordinates [longitude, latitude]. For details, please see [https://geojson.org/](https://geojson.org/).


```json
{
  "type": "Feature",
  "geometry": {
    "type": "Point",
    "coordinates": [139.8107, 35.7101]
  },
  "properties": {
    "name": "Tokyo Skytree"
  }
}
```

As for the stream in CSV format, the first and second attribute must be longitude and latitude, respectively.

All queries illustrated in this section make use of aggregation windows and are continuous in nature, i.e., they generate window-based continuous results on the continuous data stream. Namely, one output is generated per window aggregation.

In the following code snippets, longitude is referred as X and latitude as Y. 

<a name="gridIndex"></a>
### Creating a Grid Index
Before running queries on GeoFlink, a grid index needs to be defined. These are the spatial bounds where all of the spatial objects and query points are expected to lie.  This step generates a grid index, which forms the backbone of GeoFlink's optimized query processing. 

The Grid index is constructed by partitioning the 2D space given by its boundary *(MinX, MinY), (MaxX, MaxY)*  *(MaxX-MinX = MaxY-MinY)* into square shaped cells of length *l*. Smaller *l* results in the finer data distribution and pruning. However, very small *l* increases number of cells exponentially incurring higher processing costs and lowering throughput. 

In the following example, the grid spans over Beijing, China. 
```
/Defining dataStream boundaries & creating index
double minX = 115.50, maxX = 117.60, minY = 39.60, maxY = 41.10;
int gridSize = 100;
UniformGrid  uGrid = new UniformGrid(gridSize, minX, maxX, minY, maxY);
```
where *GridSize* of 100 generates a grid index of 100x100 cells, with the bottom-left (minX, minY) and top-right (maxX, maxY) coordinates, respectively.

<a name="defineSpatialStream"></a>
### Defining a Spatial Data Stream
GeoFlink users need to make an appropriate Apache Kafka connection by specifying the topic name and bootstrap server(s). Once the connection is established, the user can construct spatial stream using the GeoFlink Java/Scala API. 

Currently, GeoFlink supports only 'point' type objects. A Point class is a GeoFlink class that initializes incoming spatial data by creating it's Point object using x,y coordinates.
```
//Kafka GeoJSON stream to Spatial points stream
DataStream<Point> spatialStream = SpatialStream.PointStream(kafkaStream, "GeoJSON", uGrid);
```
where *uGrid* is the grid index.

<a name="range"></a>
### Continuous Spatial Range Query
Given a data stream *S*, query point *q*, radius *r* and window parameters, range query returns the *r*-neighbours of *q* in *S* for each aggregation window.

To execute a spatial range query via GeoFlink, Java/Scala API  *SpatialRangeQuery* method of the *RangeQuery* class is used.

GeoFlink supports three kinds of spatial range queries.
1- Point-Point
2- Point-Polygon
3- Polygon-Polygon

<a name="pointPointRange"></a>
#### 1-Point-Point Spatial Range Query
Both the query and the spatial stream consist of point objects. An example of point-point spatial range query is as follows:
```
// Query point creation using coordinates (longitude, latitude)
Point queryPoint = new Point(116.414899, 39.920374, uGrid); 

// Continous range query 
int windowSize = 10; // window size in seconds
int windowSlideStep = 5; // window slide step in seconds
int queryRadius = 0.5;

DataStream<Point> rNeighborsStream = RangeQuery.SpatialRangeQuery(spatialStream, queryPoint, queryRadius, windowSize, windowSlideStep, uGrid); 
```
where *spatialStream* is a spatial data stream, *queryPoint* denotes a query point, *radius* denotes the range query radius, *windowSize* and *windowSlideStep* denote the sliding window size and slide step respectively and *uGrid* denotes the grid index. The query output is generated continuously for each window slide based on size and slide step.

<a name="pointPolygonRange"></a>
#### 2-Point-Polygon Spatial Range Query
The query is a point object and the spatial stream consists of polygon objects. An example of point-polygon spatial range query is as follows:
```
// Query point creation using coordinates (longitude, latitude)
Point queryPoint = new Point(116.414899, 39.920374, uGrid); 

// Continous range query 
int windowSize = 10; // window size in seconds
int windowSlideStep = 5; // window slide step in seconds
int queryRadius = 0.5;

DataStream<Polygon> pointPolygonRangeQueryOutput = RangeQuery.SpatialRangeQuery(spatialPolygonStream, queryPoint, radius, uGrid, windowSize, windowSlideStep); 
```
where *spatialStream* is a spatial data stream, *queryPoint* denotes a query point, *radius* denotes the range query radius, *windowSize* and *windowSlideStep* denote the sliding window size and slide step respectively and *uGrid* denotes the grid index. The query output is generated continuously for each window slide based on size and slide step.

<a name="polygonPolygonRange"></a>
#### 3-Polygon-Polygon Spatial Range Query
The query is a polygon object and the spatial stream consists of polygon objects. An example of polygon-polygon spatial range query is as follows:
```
// Query polygon creation using coordinates (longitude, latitude)
ArrayList<Coordinate> queryPolygonCoordinates = new ArrayList<Coordinate>();  
queryPolygonCoordinates.add(new Coordinate(-73.984416, 40.675882));  
queryPolygonCoordinates.add(new Coordinate(-73.984511, 40.675767));  
queryPolygonCoordinates.add(new Coordinate(-73.984719, 40.675867));  
queryPolygonCoordinates.add(new Coordinate(-73.984726, 40.67587));  
queryPolygonCoordinates.add(new Coordinate(-73.984718, 40.675881));  
queryPolygonCoordinates.add(new Coordinate(-73.984631, 40.675986));  
queryPolygonCoordinates.add(new Coordinate(-73.984416, 40.675882));  
Polygon queryPolygon = new Polygon(queryPolygonCoordinates, uGrid);

// Continous range query 
int windowSize = 10; // window size in seconds
int windowSlideStep = 5; // window slide step in seconds
int queryRadius = 0.5;

DataStream<Polygon> polygonPolygonRangeQueryOutput = RangeQuery.SpatialRangeQuery(spatialPolygonStream, queryPolygon, radius, uGrid, windowSize, windowSlideStep);
```
where *spatialStream* is a spatial data stream, *queryPoint* denotes a query point, *radius* denotes the range query radius, *windowSize* and *windowSlideStep* denote the sliding window size and slide step respectively and *uGrid* denotes the grid index. The query output is generated continuously for each window slide based on size and slide step.


<a name="kNN"></a>
<a name="FkNN"></a>
### Continuous Spatial *k*NN Query
Given a data stream *S*, a query point *q*, a query radius *r*, a positive integer *k* and window parameters, *k*NN query returns the nearest *k* *r*-neighbours of *q* in *S* for each slide of the aggregation window. If less than *k* nearest neighbours of *q* lie within the *r* distance of *q* in *S*, then all the *r*-neighbours are returned.

To execute a spatial *k*NN query in GeoFlink, *SpatialKNNQuery* method of the *KNNQuery* class is used.
```
// Query point creation
Point queryPoint = new Point(116.414899, 39.920374, uGrid);

// Define input parameters
k = 50
int windowSize = 10; // window size in seconds
int windowSlideStep = 5; // window slide step in seconds
int queryRadius = 0.5;

// Register a spatial kNN query
DataStream <PriorityQueue<Tuple2<Point, Double>>> outputStream = KNNQuery.SpatialKNNQuery(spatialStream, queryPoint, queryRadius, k, windowSize, windowSlideStep, uGrid);
```
where *k* denotes the number of points required in the *k*NN output, *spatialStream* denotes a spatial data stream, *queryPoint* denotes a query point,  *queryRadius* denotes the max query radius to search for *k*NN, *windowSize* and *windowSlideStep* denote the sliding window size and slide step respectively and *uGrid* denotes the grid index. 


 Please note that the output stream is a stream of priority queue which is a sorted list of *k*NNs with respect to the distance from the query point *q*. The query output is generated continuously for each window slide based on the *windowSize* and *windowSlideStep*.

<!--
<a name="IkNN"></a>
### Continuous Iterative Spatial *k*NN Query

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
 -->


<a name="join"></a>
### Continuous Spatial Join Query
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

<a name="gettingStarted"></a>
## Getting Started
<a name="requirements"></a>
 ### Requirements
 - Java 8
 - Maven 3.0.4 (or higher)
- Scala 2.11 or 2.12 (optional for running Scala API)
- Apache Flink cluster v.1.9.x or higher
- Apache Kafka cluster  v.2.x.x

Please ensure that your Apache Flink and Apache Kafka clusters are configured correctly before running GeoFlink. 

<a name="firstJob"></a>
### Running Your First GeoFlink Job
- Set up your Kafka cluster and load it with a spatial data stream.
- Download or clone the GeoFlink from [https://github.com/salmanahmedshaikh/GeoFlink](https://github.com/salmanahmedshaikh/GeoFlink).
- Use your favourite IDE to open the downloaded GeoFlink project. We recommend using intelliJ Idea IDE.
- Use ``` StreamingJob ``` class to write your custom code utilizing the GeoFlink's methods discussed above. In the following we provide a sample GeoFlink code for a spatial range query.
- One can use IntelliJ IDE to execute the GeoFlink's project on a single node.
- For a cluster execution, a project's jar file need to be created. To generate the ```.jar``` file, go to the project directory through command line and run ```mvn clean package```.
- The ```.jar``` file can be uploaded and executed through the flink WebUI usually available at [http://localhost:8081](http://localhost:8081/).  

<a name="sampleCode"></a>
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

<a name="publications"></a>
## Publications

 - GeoFlink @ CIKM2020 [GeoFlink: A Distributed and Scalable Framework for the Real-time Processing of Spatial Streams](https://dl.acm.org/doi/10.1145/3340531.3412761) 
 

<a name="contact"></a>
## Contact Us!
For queries and suggestions, please contact us @ shaikh.salman@aist.go.jp
