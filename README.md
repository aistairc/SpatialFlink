# GeoFlink 

GeoFlink extends Apache Flink to support spatial data types, index and continuous queries over data streams. To enable the efficient processing of continuous spatial queries and for the effective data distribution among the Flink cluster nodes, a gird-based index is used. GeoFlink currently supports spatial range, spatial kNN and spatial join queries on point data type.

## A sample GeoFlink code written in Java to execute Range Query on a spatial data stream

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
