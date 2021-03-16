package GeoFlink.spatialObjects;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class LineString extends SpatialObject implements Serializable {

    public HashSet<String> gridIDsSet;
    public String gridID;
    public Tuple2<Coordinate, Coordinate> boundingBox;
    public org.locationtech.jts.geom.LineString lineString;

    public LineString() {}; // required for POJO

    public LineString(String objID, List<Coordinate> coordinates, HashSet<String> gridIDsSet, String gridID, Tuple2<Coordinate, Coordinate> boundingBox) {
        if (coordinates.size() > 1) {
            GeometryFactory geofact = new GeometryFactory();
            //create geotools point object
            lineString = geofact.createLineString(coordinates.toArray(new Coordinate[0]));
            this.gridIDsSet = gridIDsSet;
            this.gridID = gridID;
            this.objID = objID;
            this.boundingBox = boundingBox;
        }
    }

    public LineString(String objID, List<Coordinate> coordinates, HashSet<String> gridIDsSet, String gridID, long timestamp, Tuple2<Coordinate, Coordinate> boundingBox) {
        if (coordinates.size() > 1) {
            GeometryFactory geofact = new GeometryFactory();
            //create geotools point object
            lineString = geofact.createLineString(coordinates.toArray(new Coordinate[0]));
            this.gridIDsSet = gridIDsSet;
            this.gridID = gridID;
            this.objID = objID;
            this.boundingBox = boundingBox;
            this.timeStampMillisec = timestamp;
        }
    }

    public LineString(String objID, org.locationtech.jts.geom.LineString lineString, UniformGrid uGrid) {
        if (lineString.getNumPoints() > 1) {
            GeometryFactory geofact = new GeometryFactory();
            //lineString = geofact.createLineString(coordinates.toArray(new Coordinate[0]));
            this.boundingBox = HelperClass.getBoundingBox(lineString);
            this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
            this.gridID = "";
            this.objID = objID;
        }
    }

    public LineString(String objID, List<Coordinate> coordinates, UniformGrid uGrid) {
        if (coordinates.size() > 1) { // LineString can only be made with 2 or more points
            GeometryFactory geofact = new GeometryFactory();
            lineString = geofact.createLineString(coordinates.toArray(new Coordinate[0]));
            this.boundingBox = HelperClass.getBoundingBox(lineString);
            this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
            this.gridID = "";
            this.objID = objID;
        }
    }

    public LineString(List<Coordinate> coordinates, UniformGrid uGrid) {
        if (coordinates.size() > 1) { // LineString can only be made with 2 or more points
            GeometryFactory geofact = new GeometryFactory();
            lineString = geofact.createLineString(coordinates.toArray(new Coordinate[0]));
            this.boundingBox = HelperClass.getBoundingBox(lineString);
            this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
            this.gridID = "";
            this.objID = "";
        }
    }

    public LineString(String objID, List<Coordinate> coordinates, HashSet<String> gridIDsSet) {
        if (coordinates.size() > 1) { // LineString can only be made with 2 or more points
            GeometryFactory geofact = new GeometryFactory();
            lineString = geofact.createLineString(coordinates.toArray(new Coordinate[0]));
            this.boundingBox = HelperClass.getBoundingBox(lineString);
            this.gridIDsSet = gridIDsSet;
            this.gridID = "";
            this.objID = objID;
        }
    }

    public LineString(String objID, List<Coordinate> coordinates) {
        if (coordinates.size() > 1) { // LineString can only be made with 2 or more points
            GeometryFactory geofact = new GeometryFactory();
            lineString = geofact.createLineString(coordinates.toArray(new Coordinate[0]));
            this.boundingBox = HelperClass.getBoundingBox(lineString);
            this.gridIDsSet = null;
            this.gridID = "";
            this.objID = objID;
        }
    }
    public LineString(String objID, List<Coordinate> coordinates, long timeStampMillisec, UniformGrid uGrid) {
        if (coordinates.size() > 1) { // LineString can only be made with 2 or more points
            GeometryFactory geofact = new GeometryFactory();
            lineString = geofact.createLineString(coordinates.toArray(new Coordinate[0]));
            this.boundingBox = HelperClass.getBoundingBox(lineString);
            this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
            this.gridID = "";
            this.objID = objID;
            this.timeStampMillisec = timeStampMillisec;
        }
    }

    //{"geometry": {"coordinates": [[[[-73.817854, 40.81909], [-73.817924, 40.819207], [-73.817791, 40.819253], [-73.817785, 40.819255], [-73.817596, 40.81932], [-73.81752, 40.819194], [-73.817521, 40.819193], [-73.817735, 40.819119], [-73.817755, 40.819113], [-73.817771, 40.819107], [-73.817798, 40.819098], [-73.817848, 40.81908], [-73.817852, 40.819087]]]], "type": "LineString"}, "type": "Feature"}

    // To print the point coordinates
    @Override
    public String toString() {
        try{
            String str = "{\"geometry\":{\"coordinates\": [";
            Coordinate[] coordinates = lineString.getCoordinates();
            for(Coordinate coordinate: coordinates)
                str = str + "[" + coordinate.getX()  + ", " + coordinate.getY() + "],";
            str = str + "], \"type\": \"LineString\"}}";
            str = str + ", " + "ObjID: " + this.objID;
            str = str + ", " + this.timeStampMillisec;
            //str = str + ", Bounding Box: " + this.boundingBox;
            //str = str + ", Grid ID: " + this.gridIDsSet;
            //str = str + ", Obj ID: " + this.objID;
            return str;
        }
        catch(NullPointerException e)
        {
            System.out.print("NullPointerException Caught");
        }
        return "Invalid Tuple";
    }
}
