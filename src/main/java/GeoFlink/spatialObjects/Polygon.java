package GeoFlink.spatialObjects;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.utils.HelperClass;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class Polygon extends SpatialObject implements Serializable {

    public HashSet<String> gridIDsSet;
    public String gridID;
    //public long lObjID;
    public Tuple2<Coordinate, Coordinate> boundingBox;
    public org.locationtech.jts.geom.Polygon polygon;

    public Polygon() {}; // required for POJO

    public Polygon(List<List<Coordinate>> coordinates, String objID, HashSet<String> gridIDsSet, String gridID, Tuple2<Coordinate, Coordinate> boundingBox) {
        if (coordinates.size() >= 1 && coordinates.get(0).size() > 3) {
            //GeometryFactory geofact = new GeometryFactory();
            //create geotools point object
            polygon = createPolygon(coordinates);
            this.gridIDsSet = gridIDsSet;
            this.gridID = gridID;
            this.objID = objID;
            this.boundingBox = boundingBox;
            this.timeStampMillisec = timeStampMillisec;
        }
    }

    public Polygon(List<List<Coordinate>> coordinates, String objID, HashSet<String> gridIDsSet, String gridID, long timeStampMillisec, Tuple2<Coordinate, Coordinate> boundingBox) {
        if (coordinates.size() >= 1 && coordinates.get(0).size() > 3) {
            //GeometryFactory geofact = new GeometryFactory();
            //create geotools point object
            polygon = createPolygon(coordinates);
            this.gridIDsSet = gridIDsSet;
            this.gridID = gridID;
            this.objID = objID;
            this.boundingBox = boundingBox;
            this.timeStampMillisec = timeStampMillisec;
        }
    }

    public Polygon(List<List<Coordinate>> coordinates, UniformGrid uGrid) {
        if (coordinates.size() >= 1 && coordinates.get(0).size() > 3) {
            /*
            //GeometryFactory geofact = new GeometryFactory();
            polygon = createPolygonArray(coordinates);
            this.boundingBox = HelperClass.getBoundingBox(polygon.get(0));
            */
            GeometryFactory geofact = new GeometryFactory();
            polygon = createPolygon(coordinates);
            this.boundingBox = HelperClass.getBoundingBox(polygon);
            this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
            this.gridID = "";
            this.objID = null;
        }
    }

    public Polygon(List<List<Coordinate>> coordinates, long timeStampMillisec, UniformGrid uGrid) {
        if (coordinates.size() >= 1 && coordinates.get(0).size() > 3) {
/*
            //GeometryFactory geofact = new GeometryFactory();
            polygon = createPolygonArray(coordinates);
            this.boundingBox = HelperClass.getBoundingBox(polygon.get(0));
 */
            GeometryFactory geofact = new GeometryFactory();
            polygon = createPolygon(coordinates);
            this.boundingBox = HelperClass.getBoundingBox(polygon);
            this.timeStampMillisec = timeStampMillisec;
            this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
            this.gridID = "";
            this.objID = null;
        }
    }

    public Polygon(String objID, List<List<Coordinate>> coordinates, long timeStampMillisec, UniformGrid uGrid) {
        if (coordinates.size() >= 1 && coordinates.get(0).size() > 3) {
/*
            //GeometryFactory geofact = new GeometryFactory();
            polygon = createPolygonArray(coordinates);
            this.boundingBox = HelperClass.getBoundingBox(polygon.get(0));
 */
            GeometryFactory geofact = new GeometryFactory();
            polygon = createPolygon(coordinates);
            this.boundingBox = HelperClass.getBoundingBox(polygon);
            this.timeStampMillisec = timeStampMillisec;
            this.gridIDsSet = HelperClass.assignGridCellID(this.boundingBox, uGrid);
            this.gridID = "";
            this.objID = objID;
        }
    }

    protected List<List<Coordinate>> getCoordinates(org.locationtech.jts.geom.Polygon polygon) {
        List<List<Coordinate>> list = new ArrayList();
        list.add(new ArrayList<Coordinate>(Arrays.asList(polygon.getExteriorRing().getCoordinates())));
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            list.add(new ArrayList<Coordinate>(Arrays.asList(polygon.getInteriorRingN(i).getCoordinates())));
        }
        return list;
    }

    public List<List<Coordinate>> getCoordinates() {
        return getCoordinates(this.polygon);
    }

    private List<org.locationtech.jts.geom.Polygon> createPolygonArray(List<List<Coordinate>> coordinates) {
        List<org.locationtech.jts.geom.Polygon> listPolygon = new ArrayList<org.locationtech.jts.geom.Polygon>();
        GeometryFactory geofact = new GeometryFactory();
        for (List<Coordinate> listCoordinate : coordinates) {
            if (listCoordinate.size() > 0 && listCoordinate.size() < 4) {
            	System.out.println("listCoordinate " + listCoordinate);
            	for (int i = 0; i < 4; i++) {
            		listCoordinate.add(listCoordinate.get(0));
            	}
            }
            if (!listCoordinate.get(0).equals(listCoordinate.get(listCoordinate.size() - 1))) {
                listCoordinate.add(listCoordinate.get(0));
            }
            org.locationtech.jts.geom.Polygon poly
                    = geofact.createPolygon(listCoordinate.toArray(new Coordinate[0]));

            if (listPolygon.size() == 0) {
                listPolygon.add(poly);
            } else if (listPolygon.get(listPolygon.size() - 1).getArea() >= poly.getArea()) {
                listPolygon.add(poly);
            } else {
                for (int i = 0; i < listPolygon.size(); i++) {
                    if (listPolygon.get(i).getArea() <= poly.getArea()) {
                        listPolygon.add(i, poly);
                        break;
                    }
                }
            }
        }
        return listPolygon;
    }

    protected org.locationtech.jts.geom.Polygon createPolygon(List<List<Coordinate>> coordinates) {
        GeometryFactory geofact = new GeometryFactory();
        if (coordinates.size() == 1) {
            List<Coordinate> listCoordinate = coordinates.get(0);
            if (!listCoordinate.get(0).equals(listCoordinate.get(listCoordinate.size() - 1))) {
                listCoordinate.add(listCoordinate.get(0));
            }
            return geofact.createPolygon(listCoordinate.toArray(new Coordinate[0]));
        }
        else {
            List<org.locationtech.jts.geom.Polygon> listPolygon = createPolygonArray(coordinates);
            LinearRing shell = geofact.createLinearRing(listPolygon.get(0).getCoordinates());
            LinearRing[] holes = new LinearRing[listPolygon.size() - 1];
            for (int i = 0; i < holes.length; i++) {
                holes[i] = geofact.createLinearRing(listPolygon.get(i + 1).getCoordinates());
            }
            return geofact.createPolygon(shell, holes);
        }
    }


    //{"geometry": {"coordinates": [[[[-73.817854, 40.81909], [-73.817924, 40.819207], [-73.817791, 40.819253], [-73.817785, 40.819255], [-73.817596, 40.81932], [-73.81752, 40.819194], [-73.817521, 40.819193], [-73.817735, 40.819119], [-73.817755, 40.819113], [-73.817771, 40.819107], [-73.817798, 40.819098], [-73.817848, 40.81908], [-73.817852, 40.819087], [-73.817854, 40.81909]]]], "type": "MultiPolygon"}, "type": "Feature"}

    // To print the point coordinates
    @Override
    public String toString() {
        try{
            String str = "{\"geometry\":{\"coordinates\": [";
            List<List<Coordinate>> listCoordinates = getCoordinates();
            for (List<Coordinate> coordinates : listCoordinates) {
                str = str + "[";
                for (Coordinate coordinate : coordinates)
                    str = str + "[" + coordinate.getX() + ", " + coordinate.getY() + "],";
                str = str.substring(0, str.length() - 1);
                str = str + "],";
            }
            str = str.substring(0, str.length() - 1);
            str = str + "], \"type\": \"Polygon\"}}";
            str = str + ", " + "ObjID: " + this.objID;
            str = str + ", " + "TimeStamp(ms): " + this.timeStampMillisec;
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
