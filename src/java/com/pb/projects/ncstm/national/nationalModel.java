package com.pb.projects.ncstm.national;

import org.apache.log4j.Logger;

import java.util.ResourceBundle;

/**
 * Model to simulate national truck flows for the North Carolina Statewide Model (NCSTM)
 * Author: Rolf Moeckel, PB Albuquerque
 * Data: 29 July 2011
 */
public class nationalModel {

    static Logger logger = Logger.getLogger(nationalModel.class);
    ResourceBundle appRb;

    public nationalModel (ResourceBundle rp) {
        // Constructor
        this.appRb = rp;
    }


    public void run (int year, int modelType) {
        // Run method for national model

        if (modelType == 2) {
            logger.info("Started national truck model for year " + year);
            longDistanceTruck ldt = new longDistanceTruck(appRb, year);
            ldt.run();
        } else if (modelType == 3) {
            logger.info("Started national auto model for year " + year);
            longDistanceAuto lda = new longDistanceAuto(appRb);
            lda.run(year);
        }
    }

}
