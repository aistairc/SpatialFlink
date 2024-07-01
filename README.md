# GeoFlink and TStream: Distributed Frameworks for the Real-Time Processing of Spatial Data Streams and Trajectory Streams, Respectively

## Table of Contents

- [Introduction](#introduction)
- [GeoFlink: Spatial Stream Processing](#geoFlinkStreamProcessing)
  * [Creating a Grid Index](#creating-a-grid-index)
  * [Defining a Spatial Data Stream](#defining-a-spatial-data-stream)
  * [Continuous Spatial Range Query](#continuous-spatial-range-query)
    + [1-Point-Point Spatial Range Query](#1-point-point-spatial-range-query)
    + [2-Point-Polygon Spatial Range Query](#2-point-polygon-spatial-range-query)
    + [3-Polygon-Polygon Spatial Range Query](#3-polygon-polygon-spatial-range-query)
  * [Continuous Spatial kNN Query](#continuous-spatial-knn-query)
  * [Continuous Spatial Join Query](#continuous-spatial-join-query)
  * [Sample GeoFlink Code for a Spatial Range Query](#sample-geoflink-code-for-a-spatial-range-query)
- [TStream: Trajectory Stream Processing](#tStreamProcessing)
  * [TStream Queries](#tstream-queries)
    + [Continuous Range Query](#continuous-range-query)
    + [Continuous kNN Query](#continuous-knn-query)
    + [Continuous Join Query](#continuous-join-query)
- [Getting Started](#getting-started)
  * [Requirements](#requirements)
  * [Running Your First GeoFlink/TStream Job](#firstJob)
- [Publications](#publications)
- [Contact Us!](#contact)



## Introduction
<a name="intro"></a>
**GeoFlink** is an extension of Apache Flink — a scalable opensource distributed streaming engine — for the real-time processing of unbounded spatial streams. GeoFlink leverages a grid-based index for preserving spatial data proximity and pruning of objects which cannot be part of a spatial query result. Thus, providing effective data distribution that guarantees reduced query processing time.

GeoFlink supports spatial range, spatial *k*NN and spatial join queries. Please refer to the [ Publications ](#publications) section for details of the architecture and experimental study demonstrating GeoFlink achieving higher query performance than other ordinary distributed approaches.

**TStream** is a distributed and scalable open source framework for the real-time processing of trajectory data streams. TStream supports range, *k*NN and join queries on trajectory streams.

<a name="geoFlinkStreamProcessing"></a>
## GeoFlink: Spatial Stream Processing

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


<a name="tStreamProcessing"></a>
## TStream: Trajectory Stream Processing
TStream takes trajectory stream as input and generates a stream of sub-trajectories corresponding to the window size.

<a name="tStreamQueries"></a>
### TStream Queries
- Range
- *k*NN
- Join


<a name="gettingStarted"></a>
## Getting Started
<a name="requirements"></a>
 ### Requirements
- Maven 3.8.6
- Java 1.8.0_333
- Scala 2.11
- Apache Flink 1.9.1
- Apache Kafka 2.13-3.2.1

Please ensure that your Apache Flink and Apache Kafka clusters are configured correctly before running GeoFlink. 

<a name="firstJob"></a>
### Running Your First GeoFlink/TStream Job
- Set up your Kafka cluster and load it with a spatial data stream.
- Download or clone the GeoFlink from [https://github.com/salmanahmedshaikh/GeoFlink](https://github.com/salmanahmedshaikh/GeoFlink).
- Use your favourite IDE to open the downloaded GeoFlink project. We recommend using intelliJ Idea IDE.
- Use ``` StreamingJob ``` class to write your custom code utilizing the GeoFlink's methods discussed above. In the following we provide a sample GeoFlink code for a spatial range query.
- One can use IntelliJ IDE to execute the GeoFlink's project on a single node.
- For a cluster execution, a project's jar file need to be created. To generate the ```.jar``` file, go to the project directory through command line and run ```mvn clean package```.
- The ```.jar``` file can be uploaded and executed through the flink WebUI usually available at [http://localhost:8081](http://localhost:8081/).  

<a name="publications"></a>
## Publications

 - GeoFlink @ CIKM2020 [GeoFlink: A Distributed and Scalable Framework for the Real-time Processing of Spatial Streams](https://dl.acm.org/doi/10.1145/3340531.3412761) 
 - GeoFlink @ IEEE Access2022 [GeoFlink: An Efficient and Scalable Spatial Data Stream Management System](https://doi.org/10.1109/ACCESS.2022.3154063) 
 

<a name="contact"></a>
## Contact Us!
For queries and suggestions, please contact us @ shaikh.salman@aist.go.jp
