package GeoFlink.spatialObjects;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GeometryCollection extends SpatialObject implements Serializable {
    public org.locationtech.jts.geom.GeometryCollection geometryCollection;
    private List<SpatialObject> listSpatialObject = new ArrayList<SpatialObject>();

    public GeometryCollection() {}; // required for POJO

    public GeometryCollection(List<SpatialObject> listSpatialObject, String objID) {
        this.geometryCollection = parseSpatialObject(listSpatialObject);
        this.objID = objID;
        this.listSpatialObject = listSpatialObject;
    }

    public GeometryCollection(List<SpatialObject> listSpatialObject, String objID, long timeStampMillisec) {
        this.geometryCollection = parseSpatialObject(listSpatialObject);
        this.objID = objID;
        this.timeStampMillisec = timeStampMillisec;
        this.listSpatialObject = listSpatialObject;
    }

    public List<SpatialObject> getSpatialObjects() {
        return this.listSpatialObject;
    }

    private org.locationtech.jts.geom.GeometryCollection parseSpatialObject(List<SpatialObject> listSpatialObject) {
        List<Geometry> listGeometry = new ArrayList<Geometry>();
        if (listSpatialObject.size() > 0) {
            for (SpatialObject obj : listSpatialObject) {
                if (obj instanceof Point) {
                    listGeometry.add(((Point) obj).point);
                }
                else if (obj instanceof MultiPolygon) {
                    listGeometry.add(((MultiPolygon) obj).getMultiPolygon());
                }
                else if (obj instanceof Polygon) {
                    listGeometry.add(((Polygon) obj).polygon);
                }
                else if (obj instanceof MultiLineString) {
                    listGeometry.add(((MultiLineString) obj).getMultiPolygon());
                }
                else if (obj instanceof LineString) {
                    listGeometry.add(((LineString) obj).lineString);
                }
            }
        }
        Geometry[] arrGeometry = listGeometry.toArray(new org.locationtech.jts.geom.Geometry[0]);
        GeometryFactory geofact = new GeometryFactory();
        return geofact.createGeometryCollection(arrGeometry);
    }

    @Override
    public String toString() {
        try {
            String str = "{\"geometry\":";
            str = str + "{\"type\": \"GeometryCollection\", ";
            str = str + "\"geometries\": [";
            for (SpatialObject obj: listSpatialObject) {
                str = str + "{\"type\": \"" + obj.getClass().getSimpleName() + "\", ";
                str = str + "\"coordinates\": [";
                if (obj instanceof Point) {
                    Point point = (Point)obj;
                    str = str + point.point.getX() + ", " + point.point.getY() + "]";
                }
                else if (obj instanceof MultiPolygon) {
                    MultiPolygon multiPolygon = (MultiPolygon)obj;
                    org.locationtech.jts.geom.Polygon[] arrPolygon = multiPolygon.arrPolygon;
                    for (org.locationtech.jts.geom.Polygon p : arrPolygon) {
                        List<List<Coordinate>> listCoordinate = multiPolygon.getCoordinates(p);
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
                    str += "]";
                }
                else if (obj instanceof Polygon) {
                    Polygon polygon = (Polygon)obj;
                    List<List<Coordinate>> listCoordinates = polygon.getCoordinates();
                    for (List<Coordinate> coordinates : listCoordinates) {
                        str = str + "[";
                        for (Coordinate coordinate : coordinates)
                            str = str + "[" + coordinate.getX() + ", " + coordinate.getY() + "],";
                        str = str.substring(0, str.length() - 1);
                        str = str + "],";
                    }
                    str = str.substring(0, str.length() - 1);
                    str += "]";
                }
                else if (obj instanceof MultiLineString) {
                    MultiLineString multiLineString = (MultiLineString)obj;
                    List<List<Coordinate>> listCoordinate = multiLineString.getListCoordinate();
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
                    str += "]";
                }
                else if (obj instanceof LineString) {
                    LineString lineString = (LineString)obj;
                    Coordinate[] coordinates = lineString.lineString.getCoordinates();
                    for(Coordinate coordinate: coordinates) {
                        str = str + "[" + coordinate.getX() + ", " + coordinate.getY() + "],";
                    }
                    str = str.substring(0, str.length() - 1);
                    str += "]";
                }
                str = str + "}, ";
            }
            if (str.endsWith(", ")) {
                str = str.substring(0, str.length() - 2);
            }
            str = str + "]}}";
            str = str + ", " + "ObjID: " + this.objID;
            str = str + ", " + "TimeStamp(ms): " + this.timeStampMillisec;
            return str;
        }
        catch(NullPointerException e)
        {
            e.printStackTrace();
        }
        return "";
    }
}
