package com.pb.projects.ncstm.external;

import java.io.PrintWriter;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;

import com.pb.common.datafile.TableDataSet;
import com.pb.common.matrix.Matrix;
import com.pb.models.processFAF.fafUtils;
import com.pb.projects.ncstm.mpoTrucks.mpoData;


/**
 * Model to simulate external truck flows for the RTC model.
 * Author: Carlee Clymer, PB Albuquerque
 * Data: 21 September 2012
 */
public class disaggreagteExternalTrips {
    static Logger logger = Logger.getLogger(disaggreagteExternalTrips.class);
    private int year;
    private ResourceBundle appRb;
    private int col = 0;
    private int[] taz,centroids;
    private TableDataSet extTruckTrips, prodAndAttr;
    private Matrix ieSUT, eiSUT, eeSUT, ieMUT, eiMUT, eeMUT, externalSUT, externalMUT;
    
    public disaggreagteExternalTrips (ResourceBundle rb, int year) {
        // Constructor
        this.appRb = rb;
        this.year = year;
    }

    public void run () {
        // Run method for external truck disaggregation.
    	
    	//get the zonal data with external stations as an array
        logger.info(" Get zonal data.");
        taz = mpoData.getZones();
        
    	//Read in external trips matrix
    	extTruckTrips = mpoData.readExtTruckTrips(appRb);
    	
    	//Read in local truck trip productions and attractions
    	String fileName = appRb.getString("production.attraction.file");
    	prodAndAttr = mpoData.readProdAndAttr(appRb,fileName);
    	
   	   	//get centroids
    	centroids = mpoData.getCentroids("Centroids");
    	
    	//Calculate E-I trips
		logger.info("Calculating EI Trips");
    	eiSUT = calculateEITrips("SUT");
    	eiMUT = calculateEITrips("MUT");    	
    	
    	//Calculate I-E trips
		logger.info("Calculating IE Trips");
    	ieSUT = calculateIETrips("SUT");
    	ieMUT = calculateIETrips("MUT");
    	
    	//Set E-E trips
		logger.info("Calculating EE Trips");
    	eeSUT = setEETrips("SUT");
    	eeMUT = setEETrips("MUT");
   	
    	//Combine all trips into external matrix
    	logger.info("Combine all trips into external matrix.");
    	externalSUT = combineExtTrkTrips("SUT");
    	externalMUT = combineExtTrkTrips("MUT");
    	
    	//Write out trip external trip table as csv
    	writeExtTrkTrips();

    }
    
    private boolean checkIfCentroid(int value){
    	for(int i = 0; i<centroids.length;i++){
    		if(centroids[i] == value) return true;
    	}
    	return false;
    }
    
	private Matrix calculateEITrips(String colName) {
		// Calculates EI Trips with production constrained
        // create seed matrix
		String attrColName = colName + "Attraction";
		if(colName.equals("SUT"))col = 3;
		if(colName.equals("MUT"))col = 4;
        Matrix mat = new Matrix("Seed", "Seed", taz.length, taz.length);
        mat.setExternalNumbers(taz);
        mat.setExternalNumbersZeroBased(taz, taz);
		for (int row = 1; row < extTruckTrips.getRowCount(); row++) {
			if(checkIfCentroid((int)extTruckTrips.getValueAt(row, 1)) && checkIfCentroid((int)extTruckTrips.getValueAt(row, 2)))continue;
			for(int cent = 0; cent<centroids.length;cent++){				
		        if((int)extTruckTrips.getValueAt(row, 2) == centroids[cent]){
		            for (int dest = 0; dest < taz.length; dest++) {	
		            	int destSMZ = taz[dest];
		            	for(int prodRow = 1; prodRow < prodAndAttr.getRowCount(); prodRow++){		            		
		            		if(prodAndAttr.getValueAt(prodRow, "Taz") == destSMZ){
				            	int colPosition = prodAndAttr.getColumnPosition(attrColName);
				            	double trips = extTruckTrips.getValueAt(row, col)*prodAndAttr.getValueAt(prodRow, attrColName)/prodAndAttr.getColumnTotal(colPosition);	
				            	//logger.info((int)extTruckTrips.getValueAt(row, 1)+", "+ destSMZ+", "+(float)trips);
				            	mat.addToValueAt(mpoData.getExtStationCorrespondence((int)extTruckTrips.getValueAt(row, 1)), destSMZ, (float)trips);
		            		} 
		            	}
		            }
		        }
			}
		}
		return mat;
	}

	private Matrix calculateIETrips(String colName) {
		// Calculates IE Trips with attractions constrained
        // create seed matrix
		String attrColName = colName + "Production";
		if(colName.equals("SUT"))col = 3;
		if(colName.equals("MUT"))col = 4;
        Matrix mat = new Matrix("Seed", "Seed", taz.length, taz.length);
        mat.setExternalNumbersZeroBased(taz, taz);
		for (int row = 1; row < extTruckTrips.getRowCount(); row++) {
			if(checkIfCentroid((int)extTruckTrips.getValueAt(row, 1)) && checkIfCentroid((int)extTruckTrips.getValueAt(row, 2)))continue;
			for(int cent = 0; cent<centroids.length;cent++){	 
		        if(extTruckTrips.getValueAt(row, 1) == centroids[cent]){
		            for (int orig = 0; orig < taz.length; orig++) {
		            	int origSMZ = taz[orig];
		            	for(int attrRow = 1; attrRow < prodAndAttr.getRowCount(); attrRow++){
		            		if(prodAndAttr.getValueAt(attrRow, "Taz") == origSMZ){
				            	int colPosition = prodAndAttr.getColumnPosition(attrColName);
				            	double trips = extTruckTrips.getValueAt(row, col)*prodAndAttr.getValueAt(attrRow, attrColName)/prodAndAttr.getColumnTotal(colPosition);	
				            	mat.addToValueAt(origSMZ, mpoData.getExtStationCorrespondence((int) extTruckTrips.getValueAt(row, 2)), (float)trips);
		            		}
		            	}
		            }
		        }
			}
		}
		return mat;
	}
	
	private Matrix setEETrips(String colName) {
		// Set the external trips
		if(colName.equals("SUT"))col = 3;
		if(colName.equals("MUT"))col = 4;
        Matrix mat = new Matrix("Seed", "Seed", taz.length, taz.length);
        mat.setExternalNumbersZeroBased(taz, taz);
        for (int row = 1; row < extTruckTrips.getRowCount(); row++) {
        	for(int cent = 0; cent<centroids.length;cent++){	
	        	if(checkIfCentroid((int)extTruckTrips.getValueAt(row, 1)) || checkIfCentroid((int)extTruckTrips.getValueAt(row, 2))) continue;
	        	mat.setValueAt(mpoData.getExtStationCorrespondence((int) extTruckTrips.getValueAt(row, 1)), 
	        			mpoData.getExtStationCorrespondence((int) extTruckTrips.getValueAt(row, 2)), 
	        					extTruckTrips.getValueAt(row, col));
        	}
        }
		return mat;
	}
	
	private Matrix combineExtTrkTrips(String trkType) {
		// Combines all external trips into a matrix
		Matrix mat = new Matrix("Seed", "Seed", taz.length, taz.length);
		mat.setExternalNumbersZeroBased(taz, taz);
        for (int r = 0; r < taz.length; ++r) {
        	int origSMZ = taz[r];
            for (int c = 0; c < taz.length; ++c) {
            	int destSMZ = taz[c];
            	if(trkType.equals("SUT")){
	            	float value = eeSUT.getValueAt(origSMZ, destSMZ) + eiSUT.getValueAt(origSMZ, destSMZ) + ieSUT.getValueAt(origSMZ, destSMZ); 
	            	mat.setValueAt(origSMZ, destSMZ, value);
            	}else if(trkType.equals("MUT")){
	            	float value = eeMUT.getValueAt(origSMZ, destSMZ) + eiMUT.getValueAt(origSMZ, destSMZ) + ieMUT.getValueAt(origSMZ, destSMZ); 
	            	mat.setValueAt(origSMZ, destSMZ, value);
                }
            }
        }              
        return mat;
	}

	private void writeExtTrkTrips() {
		// write external truck trip matrix into a .csv file
        String fileName = appRb.getString("ext.truck.model.output") + "_" + year + ".csv";
        logger.info("Writing truck trips to file " + fileName);
        PrintWriter pw = fafUtils.openFileForSequentialWriting(fileName);
        pw.println("OrigZone,DestZone,singleUnitTrucks,multiUnitTrucks");
        for (int orig = 0; orig < taz.length; orig++) {
        	int originSMZ = taz[orig];
            for (int dest = 0; dest < taz.length; dest++) {
            	int destinationSMZ = taz[dest];
//            	logger.info(eiSUT.getValueAt(originSMZ, destinationSMZ));
                pw.format ("%d,%d,%.6f,%.6f", originSMZ, destinationSMZ, externalSUT.getValueAt(originSMZ, destinationSMZ),
                		externalMUT.getValueAt(originSMZ, destinationSMZ));
                pw.println();
            }
        }
        pw.close();
	}

}