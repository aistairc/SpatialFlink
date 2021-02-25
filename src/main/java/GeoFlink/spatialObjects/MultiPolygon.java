package GeoFlink.spatialObjects;

import GeoFlink.spatialIndices.UniformGrid;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class MultiPolygon extends Polygon implements Serializable {

    private List<List<Coordinate>> listCoordinate = new ArrayList<List<Coordinate>>();

    public MultiPolygon() {}; // required for POJO

    public MultiPolygon(List<List<Coordinate>> coordinates, String objID, HashSet<String> gridIDsSet, String gridID, Tuple2<Coordinate, Coordinate> boundingBox) {
        super(coordinates, objID, gridIDsSet, gridID, boundingBox);
        this.listCoordinate = orderPolygonCoordinates(coordinates);
    }

    public MultiPolygon(List<List<Coordinate>> coordinates, long timeStampMillisec, UniformGrid uGrid) {
        super(coordinates, timeStampMillisec, uGrid);
        this.listCoordinate = orderPolygonCoordinates(coordinates);
    }

    public MultiPolygon(List<List<Coordinate>> coordinates, UniformGrid uGrid) {
        super(coordinates, uGrid);
        this.listCoordinate = orderPolygonCoordinates(coordinates);
    }

    public MultiPolygon(List<List<Coordinate>> coordinates, String objID, long timeStampMillisec, UniformGrid uGrid) {
        super(objID, coordinates, timeStampMillisec, uGrid);
        this.listCoordinate = orderPolygonCoordinates(coordinates);
    }

    private List<List<Coordinate>> orderPolygonCoordinates(List<List<Coordinate>> listCoordinate) {
        List<List<Coordinate>> list = new ArrayList<List<Coordinate>>();
        List<org.locationtech.jts.geom.Polygon> listPolygon = createPolygonArray(listCoordinate);
        for (org.locationtech.jts.geom.Polygon poly : listPolygon) {
            list.add(new ArrayList<Coordinate>(Arrays.asList(poly.getCoordinates())));
        }
        return list;
    }
    
    // To print the point coordinates
    @Override
    public String toString() {
        try{
            String str = "{\"geometry\":{\"coordinates\": [";
            for (List<Coordinate> l: listCoordinate) {
                str = str + "[[";
                for(Coordinate coordinate: l)
                    str = str + "[" + coordinate.getX()  + ", " + coordinate.getY() + "],";
                if (str.charAt(str.length() - 1) == ',') {
                    str = str.substring(0, str.length() - 1);
                }
                str += "]],";
            }
            str = str.substring(0, str.length() - 1);
            str = str + "], \"type\": \"MultiPolygon\"}}";
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
