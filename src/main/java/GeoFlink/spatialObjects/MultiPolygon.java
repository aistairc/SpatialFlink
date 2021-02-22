package GeoFlink.spatialObjects;

import GeoFlink.spatialIndices.UniformGrid;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class MultiPolygon extends Polygon implements Serializable {

    private List<List<Coordinate>> listCoordinate = new ArrayList<List<Coordinate>>();

    public MultiPolygon() {}; // required for POJO

    public MultiPolygon(List<Coordinate> coordinates, long objID, HashSet<String> gridIDsSet, String gridID, Tuple2<Coordinate, Coordinate> boundingBox) {
        super(coordinates, objID, gridIDsSet, gridID, boundingBox);
        listCoordinate.add(coordinates);
    }

    public MultiPolygon(List<Coordinate> coordinates, long timeStampMillisec, UniformGrid uGrid) {
        super(coordinates, timeStampMillisec, uGrid);
        listCoordinate.add(coordinates);
    }

    public MultiPolygon(List<List<Coordinate>> listCoordinate, UniformGrid uGrid) {
        super(listCoordinate.get(0), uGrid);
        this.listCoordinate = listCoordinate;
    }

    public MultiPolygon(List<List<Coordinate>> listCoordinate, long objID, long timeStampMillisec, UniformGrid uGrid) {
        super(objID, listCoordinate.get(0), timeStampMillisec, uGrid);
        this.listCoordinate = listCoordinate;
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
            str = str + ", " + "ObjID: " + this.lObjID;
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
