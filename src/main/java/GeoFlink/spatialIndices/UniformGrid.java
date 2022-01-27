/*
Copyright 2020 Data Platform Research Team, AIRC, AIST, Japan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package GeoFlink.spatialIndices;

import GeoFlink.spatialObjects.LineString;
import GeoFlink.spatialObjects.Point;
import GeoFlink.spatialObjects.Polygon;
import GeoFlink.utils.DistanceFunctions;
import GeoFlink.utils.HelperClass;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;

import java.io.Serializable;
import java.util.*;

public class UniformGrid implements SpatialIndex {

    double minX;     //X - East-West longitude
    double maxX;
    double minY;     //Y - North-South latitude
    double maxY;

    final private int CELLINDEXSTRLENGTH = 5;
    double cellLength;
    int numGridPartitions;
    HashSet<String> girdCellsSet = new HashSet<String>();
    //Map<String, Coordinate[]> cellBoundaries = new HashMap<>();

    // UTM coordinates in meters
    public UniformGrid(double cellLength, double minX, double maxX, double minY, double maxY){

        this.minX = minX;     //X - East-West longitude
        this.maxX = maxX;
        this.minY = minY;     //Y - North-South latitude
        this.maxY = maxY;

        this.cellLength = cellLength;
        adjustCoordinatesForSquareGrid();

        double gridLength = DistanceFunctions.getPointPointEuclideanDistance(this.minX, this.minY, this.maxX, this.minY);
        //System.out.println("gridLengthInMeters " + gridLengthInMeters);

        double numGridRows = gridLength/cellLength;
        //System.out.println("numGridRows" + numGridRows);

        if(numGridRows < 1)
            this.numGridPartitions = 1;
        else
            this.numGridPartitions = (int)Math.ceil(numGridRows);

        // Computing actual cell length after adjustCoordinatesForSquareGrid
        this.cellLength = (this.maxX - this.minX) / this.numGridPartitions;

        populateGridCells();
    }

    public UniformGrid(int uniformGridRows, double minX, double maxX, double minY, double maxY)
    {
        this.minX = minX;     //X - East-West longitude
        this.maxX = maxX;
        this.minY = minY;     //Y - North-South latitude
        this.maxY = maxY;

        this.numGridPartitions = uniformGridRows;

        this.cellLength = (this.maxX - this.minX) / uniformGridRows;
        populateGridCells();
    }

    private void populateGridCells(){

        // Populating the girdCellset - contains all the cells in the grid
        for (int i = 0; i < this.numGridPartitions; i++) {
            for (int j = 0; j < this.numGridPartitions; j++) {
                String cellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                girdCellsSet.add(cellKey);

                //System.out.println(cellKey);

                // Computing cell boundaries in terms of two extreme coordinates
                //Coordinate minCoordinate = new Coordinate(this.minX + (i * cellLength), this.minY + (j * cellLength), 0);
                //Coordinate maxCoordinate = new Coordinate(this.minX + ((i + 1) * cellLength), this.minY + ((j + 1) * cellLength), 0);

                //Coordinate[] coordinates = {minCoordinate, maxCoordinate};
                //System.out.println(cellKey + ", " + min + ", " + max);
                //cellBoundaries.put(cellKey, coordinates);
            }
        }

        /* Testing the cellBoundaries
        for (Map.Entry<String,Coordinate[]> entry : cellBoundaries.entrySet())
            System.out.println("Key = " + entry.getKey() +
                    ", Value = " + entry.getValue()[0].getX() + ", " + entry.getValue()[0].getY() );
         */
    }

    private void adjustCoordinatesForSquareGrid(){

        double xAxisDiff = this.maxX - this.minX;
        double yAxisDiff = this.maxY - this.minY;

        // Adjusting coordinates to make square grid cells
        if(xAxisDiff > yAxisDiff)
        {
            double diff = xAxisDiff - yAxisDiff;
            this.maxY += diff / 2;
            this.minY -= diff / 2;
        }
        else if(yAxisDiff > xAxisDiff)
        {
            double diff = yAxisDiff - xAxisDiff;
            this.maxX += diff / 2;
            this.minX -= diff / 2;
        }


    }

    public double getMinX() {return this.minX;}
    public double getMinY() {return this.minY;}
    public double getMaxX() {return this.maxX;}
    public double getMaxY() {return this.maxY;}
    public int getCellIndexStrLength() {return CELLINDEXSTRLENGTH;}

    public int getNumGridPartitions()
    {
        return numGridPartitions;
    }
    public double getCellLength() {return cellLength;}
    public HashSet<String> getGirdCellsSet() {return girdCellsSet;}

    public Coordinate[] getCellMinMaxBoundary(String cellKey){

        ArrayList<Integer> cellIndices = HelperClass.getIntCellIndices(cellKey);

        Coordinate minCoordinate = new Coordinate(this.minX + (cellIndices.get(0) * cellLength), this.minY + (cellIndices.get(1) * this.cellLength), 0);
        Coordinate maxCoordinate = new Coordinate(this.minX + ((cellIndices.get(0) + 1) * cellLength), this.minY + ((cellIndices.get(1) + 1) * this.cellLength), 0);
        Coordinate[] coordinates = {minCoordinate, maxCoordinate};

        return coordinates;
    }

    /*
    getGuaranteedNeighboringCells: returns the cells containing the guaranteed r-neighbors
    getCandidateNeighboringCells: returns the cells containing the candidate r-neighbors and require distance computation
    The output set of the above two functions are mutually exclusive
    */
    public HashSet<String> getGuaranteedNeighboringCells(double queryRadius, String queryGridCellID)
    {
        HashSet<String> guaranteedNeighboringCellsSet = new HashSet<String>();
        int guaranteedNeighboringLayers = getGuaranteedNeighboringLayers(queryRadius);

        // if guaranteedNeighboringLayers == -1, there is no GuaranteedNeighboringCells
        if(guaranteedNeighboringLayers == 0)
        {
            guaranteedNeighboringCellsSet.add(queryGridCellID);
        }
        else if(guaranteedNeighboringLayers > 0)
        {
            ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryGridCellID);       //converts cellID String->Integer

            for(int i = queryCellIndices.get(0) - guaranteedNeighboringLayers; i <= queryCellIndices.get(0) + guaranteedNeighboringLayers; i++)
                for(int j = queryCellIndices.get(1) - guaranteedNeighboringLayers; j <= queryCellIndices.get(1) + guaranteedNeighboringLayers; j++)
                {
                    if(validKey(i,j))
                    {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        guaranteedNeighboringCellsSet.add(neighboringCellKey);
                    }
                }
        }
        return guaranteedNeighboringCellsSet;
    }

    // Guaranteed Neighboring Cells of Polygon Query
    public HashSet<String> getGuaranteedNeighboringCells(double queryRadius, Polygon queryPolygon)
    {
        HashSet<String> gridIDsSet = queryPolygon.gridIDsSet;
        HashSet<String> guaranteedNeighboringCellsSet = new HashSet<String>();

        for(String cellID:gridIDsSet) {

            HashSet<String> guaranteedNeighbors = getGuaranteedNeighboringCells(queryRadius, cellID);
            guaranteedNeighboringCellsSet.addAll(guaranteedNeighbors);
        }

        //System.out.println("guaranteedNeighboringCellsSet Size: " + guaranteedNeighboringCellsSet);
        return guaranteedNeighboringCellsSet;
    }

    // Guaranteed Neighboring Cells of LineString
    public HashSet<String> getGuaranteedNeighboringCells(double queryRadius, LineString lineString)
    {
        HashSet<String> gridIDsSet = lineString.gridIDsSet;
        HashSet<String> guaranteedNeighboringCellsSet = new HashSet<String>();

        for(String cellID:gridIDsSet) {

            HashSet<String> guaranteedNeighbors = getGuaranteedNeighboringCells(queryRadius, cellID);
            guaranteedNeighboringCellsSet.addAll(guaranteedNeighbors);
        }

        //System.out.println("guaranteedNeighboringCellsSet Size: " + guaranteedNeighboringCellsSet);
        return guaranteedNeighboringCellsSet;
    }

    public boolean validKey(int x, int y){
        if(x >= 0 && y >= 0 && x < numGridPartitions && y < numGridPartitions)
        {return true;}
        else
        {return false;}
    }

    // Return all the neighboring cells up to the given grid layer
    public HashSet<String> getNeighboringCellsByLayer(Point p, int numNeighboringLayers)
    {
        String givenCellID = p.gridID;
        HashSet<String> neighboringCellsSet = new HashSet<String>();

        if(numNeighboringLayers <= 0)
        {
            System.out.println("candidateNeighboringLayers cannot be 0 or less");
            System.exit(1); // Unsuccessful termination
        }
        else //numNeighboringLayers > 0
        {
            ArrayList<Integer> cellIndices = HelperClass.getIntCellIndices(givenCellID);

            for(int i = cellIndices.get(0) - numNeighboringLayers; i <= cellIndices.get(0) + numNeighboringLayers; i++)
                for(int j = cellIndices.get(1) - numNeighboringLayers; j <= cellIndices.get(1) + numNeighboringLayers; j++)
                {
                    if(validKey(i,j))
                    {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        neighboringCellsSet.add(neighboringCellKey);
                    }
                }
        }
        return neighboringCellsSet;

    }

    // Return all the neighboring cells including candidate cells and guaranteed cells
    public HashSet<String> getNeighboringCells(double queryRadius, Point queryPoint)
    {
        // return all the cells in the set
        if(queryRadius == 0){
            return this.girdCellsSet;
        }

        String queryCellID = queryPoint.gridID;
        HashSet<String> neighboringCellsSet = new HashSet<String>();
        int numNeighboringLayers = getCandidateNeighboringLayers(queryRadius);

        if(numNeighboringLayers <= 0)
        {
            System.out.println("candidateNeighboringLayers cannot be 0 or less");
            System.exit(1); // Unsuccessful termination
        }
        else //numNeighboringLayers > 0
        {
            ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryCellID);

            for(int i = queryCellIndices.get(0) - numNeighboringLayers; i <= queryCellIndices.get(0) + numNeighboringLayers; i++)
                for(int j = queryCellIndices.get(1) - numNeighboringLayers; j <= queryCellIndices.get(1) + numNeighboringLayers; j++)
                {
                    if(validKey(i,j))
                    {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        neighboringCellsSet.add(neighboringCellKey);
                    }
                }

        }
        return neighboringCellsSet;
    }

    // Return all the neighboring cells including candidate cells and guaranteed cells
    public HashSet<String> getNeighboringCells(double queryRadius, Polygon queryPolygon)
    {
        // return all the cells in the set
        if(queryRadius == 0){
            return this.girdCellsSet;
        }

        String queryCellID = queryPolygon.gridID;
        System.out.println("queryCellID " + queryCellID);
        HashSet<String> neighboringCellsSet = new HashSet<String>();
        int numNeighboringLayers = getCandidateNeighboringLayers(queryRadius);

        if(numNeighboringLayers <= 0)
        {
            System.out.println("candidateNeighboringLayers cannot be 0 or less");
            System.exit(1); // Unsuccessful termination
        }
        else //numNeighboringLayers > 0
        {
            ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryCellID);

            for(int i = queryCellIndices.get(0) - numNeighboringLayers; i <= queryCellIndices.get(0) + numNeighboringLayers; i++)
                for(int j = queryCellIndices.get(1) - numNeighboringLayers; j <= queryCellIndices.get(1) + numNeighboringLayers; j++)
                {
                    if(validKey(i,j))
                    {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        neighboringCellsSet.add(neighboringCellKey);
                    }
                }

        }
        return neighboringCellsSet;
    }


    // Return all the neighboring cells including candidate cells and guaranteed cells
    public HashSet<String> getNeighboringCells(double queryRadius, LineString lineString)
    {
        // return all the cells in the set
        if(queryRadius == 0){
            return this.girdCellsSet;
        }

        String queryCellID = lineString.gridID;
        HashSet<String> neighboringCellsSet = new HashSet<String>();
        int numNeighboringLayers = getCandidateNeighboringLayers(queryRadius);

        if(numNeighboringLayers <= 0)
        {
            System.out.println("candidateNeighboringLayers cannot be 0 or less");
            System.exit(1); // Unsuccessful termination
        }
        else //numNeighboringLayers > 0
        {
            ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryCellID);

            for(int i = queryCellIndices.get(0) - numNeighboringLayers; i <= queryCellIndices.get(0) + numNeighboringLayers; i++)
                for(int j = queryCellIndices.get(1) - numNeighboringLayers; j <= queryCellIndices.get(1) + numNeighboringLayers; j++)
                {
                    if(validKey(i,j))
                    {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        neighboringCellsSet.add(neighboringCellKey);
                    }
                }

        }
        return neighboringCellsSet;
    }

    // Query Point
    public HashSet<String> getCandidateNeighboringCells(double queryRadius, String queryGridCellID, Set<String> guaranteedNeighboringCellsSet)
    {
        // queryRadius = CoordinatesConversion.metersToDD(queryRadius,cellLength,cellLengthMeters);  //UNCOMMENT FOR HAVERSINE (METERS)
        //String queryCellID = queryPoint.gridID;
        HashSet<String> candidateNeighboringCellsSet = new HashSet<String>();
        int candidateNeighboringLayers = getCandidateNeighboringLayers(queryRadius);

        if(candidateNeighboringLayers > 0)
        {
            ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryGridCellID);
            //int count = 0;

            for(int i = queryCellIndices.get(0) - candidateNeighboringLayers; i <= queryCellIndices.get(0) + candidateNeighboringLayers; i++)
                for(int j = queryCellIndices.get(1) - candidateNeighboringLayers; j <= queryCellIndices.get(1) + candidateNeighboringLayers; j++)
                {
                    if(validKey(i,j)) {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        if (!guaranteedNeighboringCellsSet.contains(neighboringCellKey)) // Add key if and only if it exist in the gridCell and is not included in the guaranteed neighbors
                        {
                            //count++;
                            candidateNeighboringCellsSet.add(neighboringCellKey);
                        }
                    }
                }
            //System.out.println("Candidate neighbouring cells: " + count);
        }
        return candidateNeighboringCellsSet;
    }


    // Query Polygon
    public HashSet<String> getCandidateNeighboringCells(double queryRadius, Polygon queryPolygon, Set<String> guaranteedNeighboringCellsSet)
    {
        HashSet<String> candidateNeighboringCellsSet = new HashSet<String>();
        HashSet<String> gridIDsSet = queryPolygon.gridIDsSet;

        for(String cellID:gridIDsSet) {
            HashSet<String> candidateNeighbors = getCandidateNeighboringCells(queryRadius, cellID, guaranteedNeighboringCellsSet);
            candidateNeighboringCellsSet.addAll(candidateNeighbors);
        }

        //System.out.println("candidateNeighboringCellsSet Size: " + candidateNeighboringCellsSet);
        return candidateNeighboringCellsSet;
    }

    // Query LineString
    public HashSet<String> getCandidateNeighboringCells(double queryRadius, LineString queryLineString, Set<String> guaranteedNeighboringCellsSet)
    {
        HashSet<String> candidateNeighboringCellsSet = new HashSet<String>();
        HashSet<String> gridIDsSet = queryLineString.gridIDsSet;

        for(String cellID:gridIDsSet) {
            HashSet<String> candidateNeighbors = getCandidateNeighboringCells(queryRadius, cellID, guaranteedNeighboringCellsSet);
            candidateNeighboringCellsSet.addAll(candidateNeighbors);
        }

        //System.out.println("candidateNeighboringCellsSet Size: " + candidateNeighboringCellsSet);
        return candidateNeighboringCellsSet;
    }

    private int getGuaranteedNeighboringLayers(double queryRadius)
    {

        double cellDiagonal = cellLength*Math.sqrt(2);

        int numberOfLayers = (int)Math.floor((queryRadius/cellDiagonal) - 1); // Subtract 1 because we do not consider the cell with the query object as a layer i
        //System.out.println("Guaranteed Number of Layers: "+ numberOfLayers );
        return numberOfLayers;
        // If return value = -1 then not even the cell containing the query is guaranteed to contain r-neighbors
        // If return value = 0 then only the cell containing the query is guaranteed to contain r-neighbors
        // If return value is a positive integer n, then the n layers around the cell containing the query is guaranteed to contain r-neighbors
    }

    public int getCandidateNeighboringLayers(double queryRadius)
    {
        int numberOfLayers = (int)Math.ceil(queryRadius/cellLength);
        return numberOfLayers;
    }

    public HashSet<String> getNeighboringLayerCells(Point queryPoint, int layerNumber)
    {
        String queryCellID = queryPoint.gridID;
        HashSet<String> neighboringLayerCellsSet = new HashSet<String>();
        HashSet<String> neighboringLayerCellsToExcludeSet = new HashSet<String>();
        ArrayList<Integer> queryCellIndices = HelperClass.getIntCellIndices(queryCellID);       //converts cellID String->Integer

        //Get the cells to exclude, iff layerNumber is greater than 0
        if(layerNumber > 0)
        {
            for(int i = queryCellIndices.get(0) - layerNumber + 1; i <= queryCellIndices.get(0) + layerNumber - 1; i++)
                for(int j = queryCellIndices.get(1) - layerNumber + 1; j <= queryCellIndices.get(1) + layerNumber -1; j++)
                {
                    if(validKey(i,j)) {
                        String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                        neighboringLayerCellsToExcludeSet.add(neighboringCellKey);
                    }
                }
        }

        for(int i = queryCellIndices.get(0) - layerNumber; i <= queryCellIndices.get(0) + layerNumber; i++)
            for(int j = queryCellIndices.get(1) - layerNumber; j <= queryCellIndices.get(1) + layerNumber; j++)
            {
                if(validKey(i,j))
                {
                    String neighboringCellKey = HelperClass.padLeadingZeroesToInt(i, CELLINDEXSTRLENGTH) + HelperClass.padLeadingZeroesToInt(j, CELLINDEXSTRLENGTH);
                    if (!neighboringLayerCellsToExcludeSet.contains(neighboringCellKey)) // Add key if and only if it exist in the gridCell
                    {
                        neighboringLayerCellsSet.add(neighboringCellKey);
                    }
                }
            }
        return neighboringLayerCellsSet;
    }

    // Returns all the neighboring layers of point p, where each layer consists of a number of cells
    public ArrayList<HashSet<String>> getAllNeighboringLayers(Point p)
    {
        ArrayList<HashSet<String>> listOfSets = new ArrayList<HashSet<String>>();

        for(int i = 0; i < numGridPartitions; i++)
        {
            HashSet<String> neighboringLayerCellSet = getNeighboringLayerCells(p, i);

            if(neighboringLayerCellSet.size() > 0)
            {
                listOfSets.add(neighboringLayerCellSet);
            }
            else
            {
                break; // break the for loop as soon as we find an empty neighboringLayerCellSet
            }
        }
        return listOfSets;
    }


    public static class getCellsFilteredByLayer extends RichFilterFunction<Tuple2<String, Integer>>
    {
        private final HashSet<String> CellIDs; // CellIDs are input parameters

        //ctor
        public getCellsFilteredByLayer(HashSet<String> CellIDs)
        {
            this.CellIDs = CellIDs;
        }

        @Override
        public boolean filter(Tuple2<String, Integer> cellIDCount) throws Exception
        {
            return CellIDs.contains(cellIDCount.f0);
        }
    }
}
