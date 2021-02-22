package GeoFlink.spatialObjects;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.Serializable;
import java.util.Arrays;
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
            this.boundingBox = HelperClass.getBoundingBox(polygon);
            this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
            this.gridID = "";
            this.objID = -1;
        }
    }

    public Polygon(List<Coordinate> coordinates, long timeStampMillisec, UniformGrid uGrid) {
        if (coordinates.size() > 1) {
            GeometryFactory geofact = new GeometryFactory();
            polygon = geofact.createPolygon(coordinates.toArray(new Coordinate[0]));
            this.boundingBox = HelperClass.getBoundingBox(polygon);
            this.timeStampMillisec = timeStampMillisec;
            this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
            this.gridID = "";
            this.objID = -1;
        }
    }

    public Polygon(long objID, List<Coordinate> coordinates, long timeStampMillisec, UniformGrid uGrid) {
        if (coordinates.size() > 1) {
            GeometryFactory geofact = new GeometryFactory();
            polygon = geofact.createPolygon(coordinates.toArray(new Coordinate[0]));
            this.boundingBox = HelperClass.getBoundingBox(polygon);
            this.timeStampMillisec = timeStampMillisec;
            this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
            this.gridID = "";
            this.objID = objID;
        }
    }


    //{"geometry": {"coordinates": [[[[-73.817854, 40.81909], [-73.817924, 40.819207], [-73.817791, 40.819253], [-73.817785, 40.819255], [-73.817596, 40.81932], [-73.81752, 40.819194], [-73.817521, 40.819193], [-73.817735, 40.819119], [-73.817755, 40.819113], [-73.817771, 40.819107], [-73.817798, 40.819098], [-73.817848, 40.81908], [-73.817852, 40.819087], [-73.817854, 40.81909]]]], "type": "MultiPolygon"}, "type": "Feature"}

    // To print the point coordinates
    @Override
    public String toString() {
        try{
            String str = "{\"geometry\":{\"coordinates\": [[";
            Coordinate[] coordinates = polygon.getCoordinates();
            for(Coordinate coordinate: coordinates)
                str = str + "[" + coordinate.getX()  + ", " + coordinate.getY() + "],";
            str = str + "]], \"type\": \"Polygon\"}}";
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
