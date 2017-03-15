package com.pb.projects.ncstm.statewide;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.Scanner;

import org.apache.log4j.Logger;

import com.pb.common.datafile.CSVFileWriter;
import com.pb.common.datafile.TableDataSet;
import com.pb.common.matrix.ColumnVector;
import com.pb.common.matrix.Matrix;
import com.pb.common.matrix.MatrixBalancerRM;
import com.pb.common.matrix.RowVector;
import com.pb.common.util.ResourceUtil;
import com.pb.projects.ncstm.ncstmData;
import com.pb.projects.ncstm.ncstmUtil;


/**
 * Model to simulate statewide truck flows for the North Carolina Statewide Model (NCSTM)
 * Author: Carlee Clymer, PB Albuquerque
 * Data: 1 August 2011
 */
public class shortDistanceTruck {

    static Logger logger = Logger.getLogger(shortDistanceTruck.class);
    ResourceBundle appRb;

    private int[] taz;
    private float[] prodMU;
    private float[] prodSU;
    private float[] prodCV;
    private String[] modeNames;
    private Matrix[] matrixArray;
    public static Matrix MultiUnit;
    public static Matrix SingleUnit;
    public static Matrix CommercialVeh;
    private ColumnVector productionMU;
    private ColumnVector productionSU;
    private ColumnVector productionCV;
    private RowVector attractionMU;
    private RowVector attractionSU;
    private RowVector attractionCV;
    private HashMap<String, Float> muParameters;
    private HashMap<String, Float> suParameters;
    private HashMap<String, Float> cvParameters;

    public shortDistanceTruck (ResourceBundle rp) {
        // Constructor
        this.appRb = rp;
    }


    public void run () {
        // Run method for short-distance truck model
        logger.info("Started short-distance truck model.");

        //Run the trip generation method
        logger.info(" Running trip generation.");
        tripGeneration();

        //Run the trip distribution method
        logger.info(" Running trip distribution.");
        tripDistribution();

    }


    private void tripGeneration(){
        //get the zonal data as an array
        logger.info(" Reading in zonal data.");
        taz = ncstmData.getZones();

        //get the trip production rates by truck class
        logger.info(" Creating hash map of parameters.");
        boolean useLnTripRates = ResourceUtil.getBooleanProperty(appRb, "use.ln.trip.rates.for.sut.mut", false);
        if (useLnTripRates) {
            muParameters = putLnTripRatesInHashMap("MultiUnit");
            suParameters = putLnTripRatesInHashMap("SingleUnit");
        } else {
            muParameters = putTripRatesInHashMap("MultiUnit");
            suParameters = putTripRatesInHashMap("SingleUnit");
        }
        cvParameters = putTripRatesInHashMap("CommercialVeh");

        //calculate the trip productions
        logger.info(" Calculating trip productions.");
        prodMU = calculateTripProductions(muParameters, useLnTripRates);
        prodSU = calculateTripProductions(suParameters, useLnTripRates);
        prodCV = calculateTripProductions(cvParameters, false);

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
        productionCV = new ColumnVector(prodCV);
        productionCV.setExternalNumbersZeroBased(taz);
        attractionCV = new RowVector(prodCV);
        attractionCV.setExternalNumbersZeroBased(taz);

        writeTripPsAndAz();
    }


    private void writeTripPsAndAz() {
        CSVFileWriter writer = new CSVFileWriter();
        TableDataSet prodAndAttr = new TableDataSet();
        File file = new File(appRb.getString("production.attraction.file"));
        prodAndAttr.appendColumn(taz, "TAZ");
        prodAndAttr.appendColumn(prodMU, "P_MultiUnit");
        prodAndAttr.appendColumn(prodSU, "P_SingleUnit");
        prodAndAttr.appendColumn(prodCV, "P_CommercialVeh");
        prodAndAttr.appendColumn(prodMU, "A_MultiUnit");
        prodAndAttr.appendColumn(prodSU, "A_SingleUnit");
        prodAndAttr.appendColumn(prodCV, "A_CommercialVeh");
        try {
            writer.writeFile(prodAndAttr, file);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    private void tripDistribution(){
        //call the trip distribution method for each truck type.
        MultiUnit = singleTypeDistribution("MultiUnit",ResourceUtil.getDoubleProperty(appRb, "mut.distribution.parameter"),productionMU, attractionMU);
//		logger.info("SUT");
        SingleUnit = singleTypeDistribution("SingleUnit",ResourceUtil.getDoubleProperty(appRb, "sut.distribution.parameter"),productionSU, attractionSU);
//		logger.info("CV");
        CommercialVeh = singleTypeDistribution("CommercialVeh",ResourceUtil.getDoubleProperty(appRb, "cv.distribution.parameter"),productionCV, attractionCV);

        //create arrays that will be read into the matrix writer method
        matrixArray = new Matrix[3];
        modeNames = new String[3];
        matrixArray = createMatrixArray(MultiUnit,SingleUnit,CommercialVeh);
        modeNames = createStringArray("MultiUnit","SingleUnit","CommercialVeh");
        writeTrucks(modeNames, matrixArray, "local.truck.model.output");
    }


    /**
     * Creates a matrix array with the trips by mode.
     * @param multiUnit
     * @param singleUnit
     * @param CommercialVeh
     * @return
     */
    private Matrix[] createMatrixArray(Matrix multiUnit, Matrix singleUnit,Matrix CommercialVeh) {
        matrixArray[0] = multiUnit;
        matrixArray[1] = singleUnit;
        matrixArray[2] = CommercialVeh;
        // matrixArray[2] = CommercialVeh.multiply(0);
        return matrixArray;
    }


    /**
     * Creates a string array with the mode names.
     * @param multiUnit
     * @param singleUnit
     * @param CommercialVeh
     * @return
     */
    private String[] createStringArray(String multiUnit, String singleUnit,String CommercialVeh) {
        modeNames[0] = multiUnit;
        modeNames[1] = singleUnit;
        modeNames[2] = CommercialVeh;
        return modeNames;
    }


    /**
     * This method creates a hash map that stores the trip rates by industry 
     * and the trip distribution parameters.
     * @param colName
     * @return
     */
    private HashMap<String, Float> putTripRatesInHashMap(String colName){
        String[] codes = ncstmData.getTripIndustries("Code");
        float[] rates = ncstmData.getTripRates(colName);
        HashMap<String, Float> tripRates = new HashMap<>();
        for (int i = 0; i < codes.length; i++)
            tripRates.put(codes[i], rates[i]);
        return tripRates;
    }


    /**
     * This method creates a hash map that stores the trip rates by industry
     * and the trip distribution parameters.
     * @param colName
     * @return
     */
    private HashMap<String, Float> putLnTripRatesInHashMap(String colName){
        String[] codes = ncstmData.getLnTripIndustries("Code");
        float[] rates = ncstmData.getLnTripRates(colName);
        HashMap<String, Float> tripRates = new HashMap<>();
        for (int i = 0; i < codes.length; i++)
            tripRates.put(codes[i], rates[i]);
        return tripRates;
    }


    /**
     * This method calculates the trip productions for the mode passed in.
     * @param tripRates
     * @return
     */
    private float[] calculateTripProductions(HashMap<String, Float> tripRates, boolean useLnTripRates){
        taz = ncstmData.getZones();
        float[] prod = new float[taz.length];
        for (int zn = 0; zn < taz.length; zn++) {
            int zone = taz[zn];
            double production = 0;
            if(ncstmData.zoneInNCSTMArea(zone) && ncstmData.getSEdataItem("POP", zone) > 0 &&
                    ncstmData.getSEdataItem("TOTEMP", zone) > 0) {
               int areaType = ncstmData.getAreaTypeOfZone(zone);
                for (String variable: tripRates.keySet()) {
                    if (!variable.contains("_ANYWHERE")) {
                        if (areaType == 1 && !variable.contains("_URB")) continue;
                        if (areaType == 2 && !variable.contains("_SUB")) continue;
                        if (areaType == 3 && !variable.contains("_RUR")) continue;
                    }
                    String seItem = variable.replace("_URB", "");
                    seItem = seItem.replace("_SUB", "");
                    seItem = seItem.replace("_RUR", "");
                    seItem = seItem.replace("_ANYWHERE", "");
                    if (useLnTripRates) {
                        // calculate logarithmic trip rate
                        production += getLnProduction(zone, seItem, tripRates.get(variable));
                    } else {
                        // calculate traditional trip rate
                        production += getProduction(zone, seItem, tripRates.get(variable));
                    }
                }
                if (useLnTripRates) {
                    prod[zn] = (float) Math.exp(production);
                } else {
                    prod[zn] = (float) production;
                }
                if (production < 0) {
                    logger.error("Calculated negative trip rate for zone " + zone + ": " + production + ". Was set to 0.");
                    prod[zn] = 0;
                }
            }
            else prod[zn] = 0;
        }
        return prod;
    }


    private double getProduction (int zone, String seItem, float tripRate) {
        // calculate trip production (traditional approach)
        if (seItem.contains("CONS")) {
            return tripRate;
        } else if (seItem.contains("DENS")) {
            float seData = ncstmData.getSEdataItem(seItem.replace("DENS", ""), zone);
            float area = ncstmData.getSEdataItem("Area", zone);
            if (area > 0) return seData / area * tripRate;
            else return 0;
        } else
        return tripRate * ncstmData.getSEdataItem(seItem, zone);
    }


    private double getLnProduction (int zone, String seItem, float tripRate) {
        // calculate trip production using a logarithmic function

        if (seItem.contains("CONS")) {
            return tripRate;
        } else if (seItem.contains("DENS")) {
            float seData = ncstmData.getSEdataItem(seItem.replace("DENS", ""), zone);
            float area = ncstmData.getSEdataItem("Area", zone);
            if (area > 0) return seData / area * tripRate;
            else return 0;
        } else {
            if (ncstmData.getSEdataItem(seItem, zone) > 0) {
                return tripRate * Math.log(ncstmData.getSEdataItem(seItem, zone));
            } else {
                return 0;
            }
        }
    }


    /**
     * This method estimates distribution for one truck type.
     * @param mode
//     * @param beta
     * @param gamma
     * @param a
     * @param b
     * @return
     */
    private Matrix singleTypeDistribution(String mode, double gamma, ColumnVector a, RowVector b) {
        // create seed matrix
        Matrix mat = new Matrix("Seed", "Seed", taz.length, taz.length);
        mat.setExternalNumbersZeroBased(taz, taz);
        MatrixBalancerRM balancer = new MatrixBalancerRM(mat,a,b,0.0001, 10, MatrixBalancerRM.ADJUST.NONE);

        for (int zoneA = 0; zoneA < taz.length; zoneA++) {
            int originSMZ = taz[zoneA];
//            logger.info(originSMZ + ","+a.getValueAt(originSMZ)+","+b.getValueAt(originSMZ));
            float[] friction = new float[taz.length];
            for (int zoneB = 0; zoneB < taz.length; zoneB++) {
                int destinationSMZ = taz[zoneB];
                float dist = ncstmData.getTruckDistance(originSMZ, destinationSMZ);
//                if(dist >50) logger.info("Origin: "+originSMZ+" Destination: "+destinationSMZ+" Dist: "+dist);
                if (dist > 50) continue;
//                logger.info("Origin: "+originSMZ+" Destination: "+destinationSMZ+" Dist: "+dist);
                if (dist == 0) dist = 0.1f;      // the friction equation crashes if distance is 0
                friction[zoneB] = (float) Math.exp(gamma * dist);
            }
            for (int zoneB = 0; zoneB < taz.length; zoneB++) {
                int destinationSMZ = taz[zoneB];
                float flow = a.getValueAt(originSMZ) * b.getValueAt(destinationSMZ) * friction[zoneB];
//                logger.info("Origin: "+originSMZ+" Destination: "+destinationSMZ+" Trips: "+flow);
                mat.setValueAt(originSMZ, destinationSMZ, flow);
            }
        }

        mat = balancer.balance();
        return mat;
    }


    /**
     * The method calls the matrix writer and writes out a TransCAD 
     * matrix.
     * @param modeNames - a string array that becomes the core names in the matrix.
     * @param modes - a matrix array containing matrices 
     */
    public void writeTrucks(String[] modeNames, Matrix[] modes, String outputToken) {

//        if (ResourceUtil.getBooleanProperty(appRb, "read.transcad.skims")) {
        // write TransCAD matrix
//        logger.info(" Writing TransCAD matrix.");
//        String fileName = ResourceUtil.getProperty(appRb, outputToken);
//        MatrixWriter writer = ncstmUtil.createTransCADWriter(appRb, fileName);
//        writer.writeMatrices(modeNames,modes);
//        } else {
            // write csv file
            logger.info(" Writing matrix of local truck trips to csv file.");
            String fileName = ResourceUtil.getProperty(appRb, outputToken).replace(".mtx", ".csv");
            PrintWriter pw = ncstmUtil.openFileForSequentialWriting(fileName);
            pw.print("orig,dest");
            for (String mode: modeNames) pw.print("," + mode);
            pw.println();
            for (int i: ncstmData.getZones()) {
                for (int j: ncstmData.getZones()) {
                    pw.print(i + "," + j);
                    for (int mode = 0; mode < modeNames.length; mode++) {
                        pw.print("," + modes[mode].getValueAt(i, j));
                    }
                    pw.println();
                }
            }
            pw.close();
    }
}