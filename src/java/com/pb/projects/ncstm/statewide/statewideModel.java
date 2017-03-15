package com.pb.projects.ncstm.statewide;

import org.apache.log4j.Logger;
import java.util.ResourceBundle;

/**
 * Model to simulate statewide truck flows for the North Carolina Statewide Model (NCSTM)
 * Author: Rolf Moeckel, PB Albuquerque
 * Data: 29 July 2011
 */
public class statewideModel {

    static Logger logger = Logger.getLogger(statewideModel.class);
    ResourceBundle appRb;

    public statewideModel (ResourceBundle rp) {
        // Constructor
        this.appRb = rp;
    }


    public void run (int year) {
        // Run method for statewide model
        logger.info("Started statewide truck model for year " + year);

        shortDistanceTruck sdt = new shortDistanceTruck(appRb);
        sdt.run();

        // Run method for statewide model
        logger.info("Finished statewide model for year " + year);
    }

}
