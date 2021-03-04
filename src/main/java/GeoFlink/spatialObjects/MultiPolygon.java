package GeoFlink.spatialObjects;

import GeoFlink.spatialIndices.UniformGrid;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class MultiPolygon extends Polygon implements Serializable {

    public org.locationtech.jts.geom.Polygon[] arrPolygon;

    public MultiPolygon() {}; // required for POJO



    /*
    public MultiPolygon(List<List<Coordinate>> coordinates, String objID, HashSet<String> gridIDsSet, String gridID, long timeStampMillisec, Tuple2<Coordinate, Coordinate> boundingBox) {
        super(coordinates, objID, gridIDsSet, gridID, timeStampMillisec, boundingBox);
        this.listCoordinate = orderPolygonCoordinates(coordinates);

     */
    public MultiPolygon(List<List<List<Coordinate>>> coordinates, String objID, HashSet<String> gridIDsSet, String gridID, Tuple2<Coordinate, Coordinate> boundingBox) {
        super(coordinates.get(0), objID, gridIDsSet, gridID, boundingBox);
        this.arrPolygon = createArrPolygon(coordinates);
    }

    public MultiPolygon(List<List<List<Coordinate>>> coordinates, long timeStampMillisec, UniformGrid uGrid) {
        super(coordinates.get(0), timeStampMillisec, uGrid);
        this.arrPolygon = createArrPolygon(coordinates);
    }

    public MultiPolygon(List<List<List<Coordinate>>> coordinates, UniformGrid uGrid) {
        super(coordinates.get(0), uGrid);
        this.arrPolygon = createArrPolygon(coordinates);
    }

    public MultiPolygon(List<List<List<Coordinate>>> coordinates, String objID, long timeStampMillisec, UniformGrid uGrid) {
        super(objID, coordinates.get(0), timeStampMillisec, uGrid);
        this.arrPolygon = createArrPolygon(coordinates);
    }

    private org.locationtech.jts.geom.Polygon[] createArrPolygon(List<List<List<Coordinate>>> coordinates) {
        List<org.locationtech.jts.geom.Polygon> listPolygon = new ArrayList<org.locationtech.jts.geom.Polygon>();
        for (List<List<Coordinate>> listCoordinates : coordinates) {
            listPolygon.add(createPolygon(listCoordinates));
        }
        return listPolygon.toArray(new org.locationtech.jts.geom.Polygon[0]);
    }

    public org.locationtech.jts.geom.MultiPolygon getMultiPolygon() {
        GeometryFactory geofact = new GeometryFactory();
        return geofact.createMultiPolygon(arrPolygon);
    }
    
    // To print the point coordinates
    @Override
    public String toString() {
        try {
            String str = "{\"geometry\":{\"coordinates\": [";
            for (org.locationtech.jts.geom.Polygon p : arrPolygon) {
                List<List<Coordinate>> listCoordinate = getCoordinates(p);
                str = str + "[";
                for (List<Coordinate> l : listCoordinate) {
                    str = str + "[";
                    for (Coordinate coordinate : l)
                        str = str + "[" + coordinate.getX() + ", " + coordinate.getY() + "],";
                    if (str.charAt(str.length() - 1) == ',') {
                        str = str.substring(0, str.length() - 1);
                    }
                    str += "],";
                }
                str = str.substring(0, str.length() - 1);
                str += "],";
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

    public List<List<List<Coordinate>>> getListCoordinate() {
        List<List<List<Coordinate>>> list = new ArrayList<List<List<Coordinate>>>();
        for (org.locationtech.jts.geom.Polygon p : arrPolygon) {
            list.add(getCoordinates(p));
        }
        return list;
    }
}
