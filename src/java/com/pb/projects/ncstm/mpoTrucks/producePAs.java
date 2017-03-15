package com.pb.projects.ncstm.mpoTrucks;

import java.io.File;
import java.io.IOException;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;

import com.pb.common.datafile.CSVFileWriter;
import com.pb.common.datafile.TableDataSet;
import com.pb.common.matrix.ColumnVector;
import com.pb.common.matrix.Matrix;
import com.pb.common.matrix.RowVector;

public class producePAs {

    private Logger logger = Logger.getLogger(producePAs.class);
    private ResourceBundle appRb;

    private int[] taz;
    private float[] prodMU,prodSU;
    public static Matrix MultiUnit,SingleUnit;

    private ColumnVector productionMU,productionSU;
    private RowVector attractionMU,attractionSU;
    
    private File paFile;

    public producePAs(ResourceBundle appRb) {
        // constructor
        this.appRb = appRb;
    }
    
    public void run () {
    	logger.info("Calculate Ps and As");
    	//get the zonal data with external stations as an array
        taz = mpoData.getZones();
	
	    //calculate the trip productions
	    logger.info(" Calculating trip productions.");
	    prodMU = calculateTripProductions("MultiUnit");
	    prodSU = calculateTripProductions("SingleUnit");
	
	    // put production and attraction factors in column vectors, production and attraction values are identical
	    logger.info(" Putting production and attraction factors into vectors.");
	    productionMU = new ColumnVector(prodMU);
	    productionMU.setExternalNumbersZeroBased(taz);
	    attractionMU = new RowVector(prodMU);
	    attractionMU.setExternalNumbersZeroBased(taz);
	    productionSU = new ColumnVector(prodSU);
	    productionSU.setExternalNumbersZeroBased(taz);
	    attractionSU = new RowVector(prodSU);
	    attractionSU.setExternalNumbersZeroBased(taz);
	
	    writeTripPsAndAz();
    }
       
    /**
     * This method calculates the trip productions for the mode passed in.
     * @param tripRates
     * @return
     */
    private float[] calculateTripProductions(String colName){
        float[] prod = new float[taz.length];
        String[] industryNames = mpoData.getTripIndustries("INDUSTRY");
        float[] rates = mpoData.getTripRates(colName);
//        logger.info("TAZ Length: "+taz.length);
        for (int zn = 0; zn < taz.length-1; zn++) {
            int zone = taz[zn];
            for (int i = 0; i < industryNames.length; i++){
//            	logger.info(industryNames[i]+", "+zone);
                prod[zn] += rates[i] * mpoData.getSEdataItem(industryNames[i], zone);
            }
        }
        return prod;
    }
    
    private void writeTripPsAndAz() {
        CSVFileWriter writer = new CSVFileWriter();
        TableDataSet prodAndAttr = new TableDataSet();
        paFile = new File(appRb.getString("production.attraction.file"));
        prodAndAttr.appendColumn(taz, "TAZ");
        prodAndAttr.appendColumn(prodMU, "MUTProduction");
        prodAndAttr.appendColumn(prodSU, "SUTProduction");
        prodAndAttr.appendColumn(prodMU, "MUTAttraction");
        prodAndAttr.appendColumn(prodSU, "SUTAttraction");
        try {
            writer.writeFile(prodAndAttr, paFile);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
	
}
