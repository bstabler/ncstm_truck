package com.pb.projects.ncstm.external;

import org.apache.log4j.Logger;

import java.util.ResourceBundle;

/**
 Model to simulate external truck flows for the 3 major mpo's in the NCSTM area
 * Author: Carlee Clymer, PB Albuquerque
 * Data: 21 September 2012
 Version 1
 */
public class externalTrips {

    private Logger logger = Logger.getLogger(externalTrips.class);
    private ResourceBundle appRb;


    public externalTrips(ResourceBundle appRb) {
        // constructor
        this.appRb = appRb;
    }


    public void run(int year) {
        // run method
        logger.info("  Started external truck model for year " + year + ".");

        disaggreagteExternalTrips det = new disaggreagteExternalTrips(appRb, year);
        det.run();
        
        logger.info("  Completed external truck model.");
    }

}