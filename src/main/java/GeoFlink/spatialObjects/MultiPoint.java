package GeoFlink.spatialObjects;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

public class MultiPoint extends SpatialObject implements Serializable {

    public HashSet<String> gridIDsSet;
    public String gridID;
    public Tuple2<Coordinate, Coordinate> boundingBox;
    public org.locationtech.jts.geom.MultiPoint multiPoint;

    public MultiPoint() {
    }

    ; // required for POJO

    public MultiPoint(String objID, List<Coordinate> coordinates, HashSet<String> gridIDsSet, String gridID, Tuple2<Coordinate, Coordinate> boundingBox) {
        GeometryFactory geofact = new GeometryFactory();
        this.multiPoint = geofact.createMultiPointFromCoords(coordinates.toArray(new Coordinate[0]));
        this.gridIDsSet = gridIDsSet;
        this.gridID = gridID;
        this.objID = objID;
        this.boundingBox = boundingBox;
        this.timeStampMillisec = timeStampMillisec;
    }

    public MultiPoint(String objID, List<Coordinate> coordinates, HashSet<String> gridIDsSet, String gridID, long timestamp, Tuple2<Coordinate, Coordinate> boundingBox) {
        Date date = new Date();
        GeometryFactory geofact = new GeometryFactory();
        this.multiPoint = geofact.createMultiPointFromCoords(coordinates.toArray(new Coordinate[0]));
        this.gridIDsSet = gridIDsSet;
        this.gridID = gridID;
        this.objID = objID;
        this.boundingBox = boundingBox;
        this.timeStampMillisec = timestamp;
    }

    public MultiPoint(String objID, org.locationtech.jts.geom.MultiPoint multiPoint, UniformGrid uGrid) {
        this.multiPoint = multiPoint;
        this.boundingBox = HelperClass.getBoundingBox(multiPoint);
        this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
        this.gridID = "";
        this.objID = objID;
    }

    public MultiPoint(String objID, List<Coordinate> coordinates, UniformGrid uGrid) {
        GeometryFactory geofact = new GeometryFactory();
        this.multiPoint = geofact.createMultiPointFromCoords(coordinates.toArray(new Coordinate[0]));
        this.boundingBox = HelperClass.getBoundingBox(this.multiPoint);
        this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
        this.gridID = "";
        this.objID = objID;
    }

    public MultiPoint(List<Coordinate> coordinates, UniformGrid uGrid) {
        GeometryFactory geofact = new GeometryFactory();
        this.multiPoint = geofact.createMultiPointFromCoords(coordinates.toArray(new Coordinate[0]));
        this.boundingBox = HelperClass.getBoundingBox(this.multiPoint);
        this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
        this.gridID = "";
        this.objID = "";
    }

    public MultiPoint(String objID, List<Coordinate> coordinates, HashSet<String> gridIDsSet) {
        GeometryFactory geofact = new GeometryFactory();
        this.multiPoint = geofact.createMultiPointFromCoords(coordinates.toArray(new Coordinate[0]));
        this.boundingBox = HelperClass.getBoundingBox(this.multiPoint);
        this.gridIDsSet = gridIDsSet;
        this.gridID = "";
        this.objID = objID;
    }

    public MultiPoint(String objID, List<Coordinate> coordinates) {
        GeometryFactory geofact = new GeometryFactory();
        this.multiPoint = geofact.createMultiPointFromCoords(coordinates.toArray(new Coordinate[0]));
        this.boundingBox = HelperClass.getBoundingBox(this.multiPoint);
        this.gridIDsSet = null;
        this.gridID = "";
        this.objID = objID;
    }

    public MultiPoint(String objID, List<Coordinate> coordinates, long timeStampMillisec, UniformGrid uGrid) {
        GeometryFactory geofact = new GeometryFactory();
        this.multiPoint = geofact.createMultiPointFromCoords(coordinates.toArray(new Coordinate[0]));
        this.boundingBox = HelperClass.getBoundingBox(this.multiPoint);
        this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
        this.gridID = "";
        this.objID = objID;
        this.timeStampMillisec = timeStampMillisec;
    }

    @Override
    public String toString() {
        try{
            String str = "{\"geometry\":{\"coordinates\": [";
            Coordinate[] coordinates = multiPoint.getCoordinates();
            for(Coordinate coordinate: coordinates)
                str = str + "[" + coordinate.getX()  + ", " + coordinate.getY() + "],";
            str = str.substring(0, str.length() - 1);
            str = str + "], \"type\": \"MultiPoint\"}}";
            str = str + ", " + "ObjID: " + this.objID;
            str = str + ", " + this.timeStampMillisec;
            //str = str + ", Bounding Box: " + this.boundingBox;
            //str = str + ", Grid ID: " + this.gridIDsSet;
            return str;
        }
        catch(NullPointerException e)
        {
            System.out.print("NullPointerException Caught");
            e.printStackTrace();
            return "";
        }
    }

}