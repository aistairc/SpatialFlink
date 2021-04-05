package GeoFlink.spatialObjects;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class MultiLineString extends LineString implements Serializable {

    private List<List<Coordinate>> listCoordinate = new ArrayList<List<Coordinate>>();
    public org.locationtech.jts.geom.LineString[] arrLineString;

    public MultiLineString() {}; // required for POJO


    public MultiLineString(String objID, List<Coordinate> coordinates, HashSet<String> gridIDsSet, String gridID, Tuple2<Coordinate, Coordinate> boundingBox) {
        super(objID, coordinates, gridIDsSet, gridID, boundingBox);
    }

    public MultiLineString(String objID, org.locationtech.jts.geom.LineString lineString, UniformGrid uGrid) {
        super(objID, lineString, uGrid);
    }

    public MultiLineString(String objID, List<List<Coordinate>> listCoordinate, UniformGrid uGrid) {
        super(objID, listCoordinate.get(0), uGrid);
        this.listCoordinate = listCoordinate;
        List<org.locationtech.jts.geom.LineString> listLineString = new ArrayList<org.locationtech.jts.geom.LineString>();
        for (List<Coordinate> list : listCoordinate) {
            GeometryFactory geofact = new GeometryFactory();
            listLineString.add(geofact.createLineString(list.toArray(new Coordinate[0])));
        }
        this.arrLineString = listLineString.toArray(new org.locationtech.jts.geom.LineString[0]);
    }

    public MultiLineString(String objID, List<List<Coordinate>> listCoordinate, long timeStampMillisec, UniformGrid uGrid) {
        super(objID, listCoordinate.get(0), timeStampMillisec, uGrid);
        this.listCoordinate = listCoordinate;
        List<org.locationtech.jts.geom.LineString> listLineString = new ArrayList<org.locationtech.jts.geom.LineString>();
       for (List<Coordinate> list : listCoordinate) {
            GeometryFactory geofact = new GeometryFactory();
            listLineString.add(geofact.createLineString(list.toArray(new Coordinate[0])));
        }
        this.arrLineString = listLineString.toArray(new org.locationtech.jts.geom.LineString[0]);
    }

    public org.locationtech.jts.geom.MultiLineString getMultiPolygon() {
        GeometryFactory geofact = new GeometryFactory();
        return geofact.createMultiLineString(arrLineString);
    }

    // To print the point coordinates
    @Override
    public String toString() {
        try{
            String str = "{\"geometry\":{\"coordinates\": [";
            for (List<Coordinate> l: listCoordinate) {
                str = str + "[";
                for(Coordinate coordinate : l)
                    str = str + "[" + coordinate.getX()  + ", " + coordinate.getY() + "],";
                if (str.charAt(str.length() - 1) == ',') {
                    str = str.substring(0, str.length() - 1);
                }
                str += "],";
            }
            str = str.substring(0, str.length() - 1);
            str = str + "], \"type\": \"MultiLineString\"}}";
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

    public List<List<Coordinate>> getListCoordinate() {
        return listCoordinate;
    }
}
