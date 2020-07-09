package GeoFlink.spatialObjects;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;

public class Polygon extends SpatialObject implements Serializable {

    public HashSet<String> gridIDsSet;
    public String gridID;
    public long objID;
    public Tuple2<Coordinate, Coordinate> boundingBox;
    public org.locationtech.jts.geom.Polygon polygon;

    public Polygon() {}; // required for POJO

    public Polygon(List<Coordinate> coordinates, long objID, HashSet<String> gridIDsSet, String gridID, Tuple2<Coordinate, Coordinate> boundingBox) {
        GeometryFactory geofact = new GeometryFactory();
        //create geotools point object
        polygon = geofact.createPolygon(coordinates.toArray(new Coordinate[0]));
        this.gridIDsSet = gridIDsSet;
        this.gridID = gridID;
        this.objID = objID;
        this.boundingBox = boundingBox;
    }

    public Polygon(List<Coordinate> coordinates, UniformGrid uGrid) {
        if (coordinates.size() > 1) {
            GeometryFactory geofact = new GeometryFactory();
            polygon = geofact.createPolygon(coordinates.toArray(new Coordinate[0]));
            this.boundingBox = HelperClass.getBoundingBox(coordinates);
            this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
            this.gridID = "";
            this.objID = -1;
        }
    }

    public Polygon(List<Coordinate> coordinates, long timeStampMillisec, UniformGrid uGrid) {
        if (coordinates.size() > 1) {
            GeometryFactory geofact = new GeometryFactory();
            polygon = geofact.createPolygon(coordinates.toArray(new Coordinate[0]));
            this.boundingBox = HelperClass.getBoundingBox(coordinates);
            this.timeStampMillisec = timeStampMillisec;
            this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
            this.gridID = "";
            this.objID = -1;
        }
    }

    // To print the point coordinates
    @Override
    public String toString() {
        try{
            String str = "";
            Coordinate[] coordinates = polygon.getCoordinates();
            for(Coordinate coordinate: coordinates)
                str = str + "(" + coordinate.getX()  + ", " + coordinate.getY() + ")";
            str = str + ", Bounding Box: " + this.boundingBox;
            str = str + ", Grid ID: " + this.gridIDsSet;
            str = str + ", Obj ID: " + this.objID;
            return str;
        }
        catch(NullPointerException e)
        {
            System.out.print("NullPointerException Caught");
        }
        return "Invalid Tuple";
    }

}
