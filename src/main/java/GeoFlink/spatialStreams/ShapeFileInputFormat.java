package GeoFlink.spatialStreams;

import GeoFlink.spatialIndices.UniformGrid;
import GeoFlink.spatialObjects.MultiLineString;
import GeoFlink.spatialObjects.Point;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;
import org.locationtech.jts.geom.Coordinate;
import GeoFlink.spatialObjects.Polygon;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;


public class ShapeFileInputFormat<E> extends FileInputFormat<E> {

    private boolean end = false;
    private UniformGrid uGrid;
    private int fileSize = 0;
    private int offset = 0;
    private static long threadId = -1;
    private static boolean open = false;
    private static Semaphore semaphore = new Semaphore(1, true);

    private static int FILE_CODE = 9994;
    private static int SHAPE_TYPE_POINT    = 1;
    private static int SHAPE_TYPE_POLYLINE = 3;
    private static int SHAPE_TYPE_POLYGON  = 5;

    public ShapeFileInputFormat(Path path, UniformGrid uGrid) {
        super(path);
        this.uGrid = uGrid;
    }

    private void getSema() {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {}
    }

    private void releaseSema() {
        semaphore.release();
    }

    @Override
    public void open(FileInputSplit fileSplit) throws IOException {
        getSema();
        long id = Thread.currentThread().getId();
        if (this.threadId == -1) {
            this.threadId = id;
        }
        releaseSema();
        super.open(fileSplit);

        this.splitStart = 0;
        this.splitLength = 0;
        this.stream.seek(this.splitStart);

        byte[] fileHeader = new byte[100];
        if (this.stream.read(fileHeader) < 0) {
            this.end = true;
            throw new IOException("Failed to read file header.");
        }
        // file code
        byte[] arrFileCode = new byte[Integer.BYTES];
        System.arraycopy(fileHeader, 0, arrFileCode, 0, arrFileCode.length);
        if(ByteBuffer.wrap(arrFileCode).getInt() != FILE_CODE) {
            this.end = true;
            throw new IOException("Illegal file. This file is not shapefile.");
        }
        // file size
        byte[] arrFileSize = new byte[Integer.BYTES];
        System.arraycopy(fileHeader, 0x18, arrFileSize, 0, arrFileSize.length);
        fileSize = ByteBuffer.wrap(arrFileSize).getInt() * 2;
        offset += fileHeader.length;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return this.end;
    }

    private static int count = 0;
    @Override
    public E nextRecord(E reuse) throws IOException {
        E ret = null;
        // read record header
        byte[] recordHeader = new byte[8];
        long id = Thread.currentThread().getId();
        if (this.threadId != id) {
            this.end = true;
            return ret;
        }
        if (this.stream.read(recordHeader) < 0) {
            this.end = true;
            throw new IOException("Failed to read record header.");
        }
        this.offset += recordHeader.length;

        // record number
        byte[] arrRecordNo = new byte[Integer.BYTES];
        System.arraycopy(recordHeader, 0x00, arrRecordNo, 0, arrRecordNo.length);
        int recordNo = ByteBuffer.wrap(arrRecordNo).getInt();
        // record size
        byte[] arrRecordSize = new byte[Integer.BYTES];
        System.arraycopy(recordHeader, 0x04, arrRecordSize, 0, arrRecordSize.length);
        int recordSize = ByteBuffer.wrap(arrRecordSize).getInt() * 2;
        // read record
        byte[] record = ByteBuffer.allocate(recordSize).array();
        if (this.stream.read(record) < 0) {
            this.end = true;
            throw new IOException("Failed to read record. Record No. [" + recordNo + "]");
        }
        // shape type
        byte[] arrShapeType = new byte[Integer.BYTES];
        System.arraycopy(record, 0, arrShapeType, 0, arrShapeType.length);
        int shapeType = ByteBuffer.wrap(arrShapeType).order(ByteOrder.LITTLE_ENDIAN).getInt();

        // shape type : Point
        if ((shapeType & 0xFF) == SHAPE_TYPE_POINT) {
            ret = (E)readPointData(record);
        }
        // shape type : Polygon
        else if ((shapeType & 0xFF) == SHAPE_TYPE_POLYGON) {
            ret = (E)readPolygonData(record);
        }
        // shape type : PolyLine
        else if ((shapeType & 0xFF) == SHAPE_TYPE_POLYLINE) {
            ret = (E)readPolyLineData(record);
        }
        else {
            System.out.println("Unsupported shape type [" + (shapeType & 0xFF) + "]");
            ret = (E)null;
        }
        return ret;
     }

    private Point readPointData(byte[] record) {
        // read coordinate
        byte[] arrX = new byte[Double.BYTES];
        System.arraycopy(record, 0x04, arrX, 0, arrX.length);
        double x = ByteBuffer.wrap(arrX).order(ByteOrder.LITTLE_ENDIAN).getDouble();
        byte[] arrY = new byte[Double.BYTES];
        System.arraycopy(record, 0x0C, arrY, 0, arrY.length);
        double y = ByteBuffer.wrap(arrY).order(ByteOrder.LITTLE_ENDIAN).getDouble();
        this.offset += record.length;

        if (this.fileSize <= this.offset) {
            this.end = true;
        }
        Point point = new Point(x, y, this.uGrid);
        return point;
    }

     private Polygon readPolygonData(byte[] record) {
         // NumParts
         byte[] arrNumParts = new byte[Integer.BYTES];
         System.arraycopy(record, 0x24, arrNumParts, 0, arrNumParts.length);
         int numParts = ByteBuffer.wrap(arrNumParts).order(ByteOrder.LITTLE_ENDIAN).getInt();

         // NumPoints
         byte[] arrNumPoints = new byte[Integer.BYTES];
         System.arraycopy(record, 0x28, arrNumPoints, 0, arrNumPoints.length);
         int numPoints = ByteBuffer.wrap(arrNumPoints).order(ByteOrder.LITTLE_ENDIAN).getInt();

         // read coordinate
         List<Coordinate> list = new ArrayList<Coordinate>();
         int offsetPoint = 0x2C + (Integer.BYTES * numParts);
         for (int i = 0; i < numPoints; i++) {
             byte[] arrX = new byte[Double.BYTES];
             System.arraycopy(record, offsetPoint, arrX, 0, arrX.length);
             double x = ByteBuffer.wrap(arrX).order(ByteOrder.LITTLE_ENDIAN).getDouble();
             offsetPoint += Double.BYTES;
             byte[] arrY = new byte[Double.BYTES];
             System.arraycopy(record, offsetPoint, arrY, 0, arrY.length);
             double y = ByteBuffer.wrap(arrY).order(ByteOrder.LITTLE_ENDIAN).getDouble();
             offsetPoint += Double.BYTES;
             list.add(new Coordinate(x, y));
         }
         this.offset += record.length;

         if (this.fileSize <= this.offset) {
             this.end = true;
         }
         Polygon polygon = new Polygon(list, this.uGrid);
         return polygon;
     }

    private MultiLineString readPolyLineData(byte[] record) {
        // NumParts
        byte[] arrNumParts = new byte[Integer.BYTES];
        System.arraycopy(record, 0x24, arrNumParts, 0, arrNumParts.length);
        int numParts = ByteBuffer.wrap(arrNumParts).order(ByteOrder.LITTLE_ENDIAN).getInt();

        // NumPoints
        byte[] arrNumPoints = new byte[Integer.BYTES];
        System.arraycopy(record, 0x28, arrNumPoints, 0, arrNumPoints.length);
        int numPoints = ByteBuffer.wrap(arrNumPoints).order(ByteOrder.LITTLE_ENDIAN).getInt();

        // Parts
        List<Integer> parts = new ArrayList<Integer>();
        for (int i = 0; i < numParts; i++) {
            byte[] arrParts = new byte[Integer.BYTES];
            System.arraycopy(record, 0x2C + (Integer.BYTES * i), arrParts, 0, arrParts.length);
            int part = ByteBuffer.wrap(arrParts).order(ByteOrder.LITTLE_ENDIAN).getInt();
            parts.add(part);
        }

        // read coordinate
        List<List<Coordinate>> list = new ArrayList<List<Coordinate>>();
        int offsetPoint = 0x2C + (Integer.BYTES * numParts);
        for (int i = 0; i < numParts; i++) {
            int partNumPoints = 0;
            if (numParts > (i + 1)) {
                partNumPoints = parts.get(i + 1) - parts.get(i);
            }
            else {
                partNumPoints = numPoints;
            }
            List<Coordinate> coordinateList = new ArrayList<Coordinate>();
            for (int j = 0; j < partNumPoints; j++) {
                byte[] arrX = new byte[Double.BYTES];
                System.arraycopy(record, offsetPoint, arrX, 0, arrX.length);
                double x = ByteBuffer.wrap(arrX).order(ByteOrder.LITTLE_ENDIAN).getDouble();
                offsetPoint += Double.BYTES;
                byte[] arrY = new byte[Double.BYTES];
                System.arraycopy(record, offsetPoint, arrY, 0, arrY.length);
                double y = ByteBuffer.wrap(arrY).order(ByteOrder.LITTLE_ENDIAN).getDouble();
                offsetPoint += Double.BYTES;
                coordinateList.add(new Coordinate(x, y));
                numPoints--;
            }
            list.add(coordinateList);
        }
        this.offset += record.length;

        if (this.fileSize <= this.offset) {
            this.end = true;
        }
        MultiLineString multiLineString = new MultiLineString(null, list, this.uGrid);
        return multiLineString;
    }
}
