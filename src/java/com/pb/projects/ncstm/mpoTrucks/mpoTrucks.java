package com.pb.projects.ncstm.mpoTrucks;

import com.pb.projects.ncstm.ncstmUtil;
import com.pb.projects.ncstm.external.externalTrips;

import org.apache.log4j.Logger;
import java.util.ResourceBundle;

/**
 * Model to simulate truck flows for the Three MPO Regions in North Carolina: Metrolina, Triad and Triangle
 * Author: Rolf Moeckel, PB Albuquerque
 * Data: 2 October 2012 (Santa Fe)
 */

public class mpoTrucks {

    static Logger logger = Logger.getLogger(mpoTrucks.class);

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();
        logger.info ("NC MPO Truck Model started.");
        ResourceBundle appRb = ncstmUtil.getResourceBundle(args[0]);
        logger.info ("Application: " + appRb.getString("application"));
        int year = Integer.valueOf(args[1]);
        String model = String.valueOf(args[2]);
        //Read in the zonal data, trip rates, and the truck skims.
        mpoData.readZonalData(appRb, year);
        mpoData.readTripRates(appRb);

        if(model.equals("ldt")){
            ldTrucks ldtT = new ldTrucks(appRb, year);
            ldtT.run();
        } if(model.equals("ext")){
	        externalTrips et = new externalTrips(appRb);
	        et.run(year);
        }

        logger.info("NC MPO Truck Model finished.");
        float runTime = ncstmUtil.rounder(((System.currentTimeMillis() - startTime) / 60000), 1);
        logger.info("Runtime: " + runTime + " minutes.");
    }

}
