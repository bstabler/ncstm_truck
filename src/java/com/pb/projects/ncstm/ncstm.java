package com.pb.projects.ncstm;
/**
 * Model to simulate truck flows for the North Carolina Statewide Model (NCSTM)
 * Author: Rolf Moeckel, PB Albuquerque
 * Data: 29 July 2011
 */

import com.pb.common.matrix.*;
import com.pb.projects.ncstm.national.nationalModel;
import com.pb.projects.ncstm.statewide.statewideModel;
import org.apache.log4j.Logger;

import java.util.ResourceBundle;


public class ncstm {

    static Logger logger = Logger.getLogger(ncstm.class);

    public static void main(String[] args) {
        // Main run method

        long startTime = System.currentTimeMillis();
        logger.info ("NCSTM started.");
        ResourceBundle appRb = ncstmUtil.getResourceBundle(args[0]);
        int year = Integer.valueOf(args[1]);
        int modelType = Integer.valueOf(args[2]);
        // report modelType info to user
        if(modelType < 0 || modelType > 3){
            logger.info("The last argument should be: 1 = sdtruck, 2 = ldtruck, or 3 = ldauto");
            System.exit(1);
        }

        switch (modelType) {
            case 1:
                logger.info("Short-distance truck model");
                break;
            case 2:
                logger.info("Long-distance truck model");
                break;
            case 3:
                logger.info("Long-distance auto model");
                break;
        }

        //Read in the zonal data, trip rates, and the truck skims.
        ncstmData.readZonalData(appRb, year);
        ncstmData.readSkims(appRb, year);
        ncstmData.readTripRates(appRb);

        if (modelType == 1) {
            statewideModel sm = new statewideModel(appRb);
            sm.run(year);
        }
        else if (modelType >= 2) {
            nationalModel nm = new nationalModel(appRb);
            nm.run(year, modelType);
        }


        logger.info("NCSTM completed.");
        float runTime = ncstmUtil.rounder(((System.currentTimeMillis() - startTime) / 60000), 1);
        logger.info("Runtime: " + runTime + " minutes.");
    }

}
