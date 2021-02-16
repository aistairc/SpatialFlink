package GeoFlink.spatialStreams;

import GeoFlink.spatialIndices.UniformGrid;
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


public class ShapeFilePolygonInputFormat<E> extends FileInputFormat<E> {

    private boolean end = false;
    private UniformGrid uGrid;
    private int fileSize = 0;
    private int offset = 0;

    private static int FILE_CODE = 9994;
    private static int SHAPE_TYPE_POLYGON = 5;

    public ShapeFilePolygonInputFormat(Path path, UniformGrid uGrid) {
        super(path);
        this.uGrid = uGrid;
    }
    @Override
    public void open(FileInputSplit fileSplit) throws IOException {
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
        // shape type
        byte[] arrShapeType = new byte[Integer.BYTES];
        System.arraycopy(fileHeader, 0x20, arrShapeType, 0, arrShapeType.length);
        int shapeType = ByteBuffer.wrap(arrShapeType).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (shapeType != SHAPE_TYPE_POLYGON) {
            this.end = true;
            throw new IOException("This file is not polygon. type[" + shapeType + "]");
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return this.end;
    }

    @Override
    public E nextRecord(E reuse) throws IOException {
        // read record header
        byte[] recordHeader = new byte[8];
        if (this.stream.read(recordHeader) < 0) {
            this.end = true;
            throw new IOException("Failed to read record header.");
        }
        offset += recordHeader.length;

        // read record
        byte[] arrRecordNo = new byte[Integer.BYTES];
        System.arraycopy(recordHeader, 0x00, arrRecordNo, 0, arrRecordNo.length);
        int recordNo = ByteBuffer.wrap(arrRecordNo).getInt();
        byte[] arrRecordSize = new byte[Integer.BYTES];
        System.arraycopy(recordHeader, 0x04, arrRecordSize, 0, arrRecordSize.length);
        int recordSize = ByteBuffer.wrap(arrRecordSize).getInt() * 2;
        byte[] record = ByteBuffer.allocate(recordSize).array();
        if (this.stream.read(record) < 0) {
            this.end = true;
            throw new IOException("Failed to read record. Record No. [" + recordNo + "]");
        }

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
        offset += recordSize;

        if (fileSize <= offset) {
            this.end = true;
        }
        Polygon polygon = new Polygon(list, this.uGrid);
        return (E)polygon;
     }
}
