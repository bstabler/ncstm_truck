package com.pb.projects.ncstm.national;

import com.pb.common.datafile.TableDataSet;
import com.pb.common.matrix.Matrix;
import com.pb.common.util.ResourceUtil;
import com.pb.models.neldt.*;
import com.pb.projects.ncstm.ncstmData;
import com.pb.projects.ncstm.ncstmUtil;
import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 * Model to simulate national auto flows for the North Carolina Statewide Transportation Model (NCSTM)
 * Author: Rolf Moeckel, PB Albuquerque
 * Data: 1 August 2011
 */
public class longDistanceAuto {

    static Logger logger = Logger.getLogger(longDistanceAuto.class);
    ResourceBundle appRb;

    public longDistanceAuto (ResourceBundle rp) {
        // Constructor
        this.appRb = rp;
    }


    public void run (int year) {
        // Run method for long-distance auto model
        logger.info("Started long-distance auto model for year " + year);
        neldt n = new neldt(appRb);
        n.readNeldtData(false);
        n.synthesizeMissingStates();
        HashMap<String,Float> scale = createScalerHashMap();
        n.expandRecords(year, scale);
        n.addInternationalVisitors(year);
        n.writeTripMatrix(year, neldtModes.auto);
        n.writeTripMatrix(year, neldtModes.bus);
        n.writeTripMatrix(year, neldtModes.train);
        // disaggregtation of long-distance travel to zonal level
        neldtDisagg nd = new neldtDisagg(appRb);
        nd.buildStateZoneReference(ncstmData.getZoneSystem(),  neldt.stateRowList);
        n.writeTripMatrix(year, neldtModes.air, findStatesToApplyR3Logit(nd));
        nd.setZonalDistances(ncstmData.getAutoDistanceSkim());
        nd.setZonalTravelTime(ncstmData.getAutoTimeSkim());
        nd.buildStateZoneReference(ncstmData.getZoneSystem(),  neldt.stateRowList);
        nd.initializeFrictionFactors();
        disaggPersonLongDistanceTravel(year, nd, n.usTravel);
        logger.info("Completed long-distance auto model.");
    }


    private HashMap<String, Float> createScalerHashMap () {
        // create HashMap with state O-D pairs that need to be scaled

        HashMap<String, Float> scaler = new HashMap<>();
        String[] tokens = ResourceUtil.getArray(appRb, "scaling.pers.trips.tokens");
        double[] values = ResourceUtil.getDoubleArray(appRb, "scaling.pers.trips.values");
        if (tokens.length != values.length) {
            throw new RuntimeException("Error.  scaling.pers.trips.tokens must be same length as scaling.pers.trips.values");
        }
        for (int i=0; i<tokens.length; i++) {
            scaler.put(tokens[i], (float) values[i]);
        }
        return scaler;
    }


    private void disaggPersonLongDistanceTravel(int year, neldtDisagg nd, float[][][][] usTravel){
        // disaggregate business travel based on population/employment (home end) and employment (destination end)
        // disaggregate personal travel based on population (home end) and population/employment (destination end)

        logger.info("  Disaggregating regional auto trips for NCSTM from states to zones for year " + year + ".");
        int autoMode = neldtUtil.getEnumOrderNumber(neldtModes.auto);
        int zones[] = ncstmData.getZones();
        nd.initializeDistStatistics();
        TableDataSet prodAttrWeights = calculateAttractionProductionWeights();
        Matrix[] allTrips = new Matrix[neldtPurposes.values().length];
        for (int p = 0; p < neldtPurposes.values().length; p++) {
            allTrips[p] = new Matrix(zones.length + 1, zones.length + 1);
            allTrips[p].setExternalNumbersZeroBased(zones, zones);
        }
        HashMap<String, Boolean> statesForMC = findStatesToApplyR3Logit(nd);
        for (int oState = 0; oState < neldt.stateRowList.length; oState++) {
            String os = neldt.stateRowList[oState];
            // exclude origins that cannot be reached by auto
            if (os.equals("CARIB") || os.equals("EUR") || os.equals("oth") || !statesForMC.containsKey(os)) continue;
            for (int dState = 0; dState < neldt.stateRowList.length; dState++) {
                String ds = neldt.stateRowList[dState];
                // exclude destinations that cannot be reached by auto
                if (ds.equals("CARIB") || ds.equals("EUR") || ds.equals("oth") || !statesForMC.containsKey(ds)) continue;
                float tripsB = usTravel[autoMode][neldtUtil.getEnumOrderNumber(neldtPurposes.business)][oState][dState];
                float tripsP = usTravel[autoMode][neldtUtil.getEnumOrderNumber(neldtPurposes.personal)][oState][dState];
                float tripsC = usTravel[autoMode][neldtUtil.getEnumOrderNumber(neldtPurposes.commute)][oState][dState];
                if (statesForMC.get(os) && statesForMC.get(ds) && (os.equals("NC") || (ds.equals("NC")))) {
                    for (int mode = 1; mode <= 3; mode++) {          // add modes air, bus and train
                        tripsB += usTravel[mode][neldtUtil.getEnumOrderNumber(neldtPurposes.business)][oState][dState];
                        tripsP += usTravel[mode][neldtUtil.getEnumOrderNumber(neldtPurposes.personal)][oState][dState];
                        tripsC += usTravel[mode][neldtUtil.getEnumOrderNumber(neldtPurposes.commute)][oState][dState];
                    }
                }
                if (tripsB + tripsP + tripsC == 0) continue;
                Matrix flowsB = nd.disaggregateStateToZoneTours(neldtPurposes.business, prodAttrWeights, os, ds, tripsB);
                Matrix flowsP = nd.disaggregateStateToZoneTours(neldtPurposes.personal, prodAttrWeights, os, ds, tripsP);
                Matrix flowsC = nd.disaggregateStateToZoneTours(neldtPurposes.commute, prodAttrWeights, os, ds, tripsC);
                int zonesRow[] = flowsB.getExternalRowNumbers();
                int zonesCol[] = flowsB.getExternalColumnNumbers();
                int zonesOrigList[] = new int[zonesRow.length - 1];
                int zonesDestList[] = new int[zonesCol.length - 1];
                System.arraycopy(zonesRow, 1, zonesOrigList, 0, zonesOrigList.length);
                System.arraycopy(zonesCol, 1, zonesDestList, 0, zonesDestList.length);
                for (int i: zonesOrigList) {
                    for (int j: zonesDestList) {
                        allTrips = addReturnTripToMatrix(neldtPurposes.business, allTrips, i, j, flowsB.getValueAt(i,j));
                        allTrips = addReturnTripToMatrix(neldtPurposes.personal, allTrips, i, j, flowsP.getValueAt(i,j));
                        allTrips = addReturnTripToMatrix(neldtPurposes.commute, allTrips, i, j, flowsC.getValueAt(i,j));
                    }
                }
            }
        }

        float sum = 0;
        for (int i = 0; i < neldtPurposes.values().length; i++) sum += allTrips[i].getSum();
        logger.info("  National total of auto long-distance travelers generated: " + Math.round(sum));
        String[] detailedStates = {"NC", "SC", "VA"};
        nd.writeOutDistStatistics(neldtModes.auto, detailedStates, year);

        Matrix[] tcMatrix = new Matrix[3];
        for (int p = 0; p < 3; p++) {
            tcMatrix[p] = new Matrix(zones.length,zones.length);
            tcMatrix[p].setExternalNumbersZeroBased(zones);
        }
        // write out trip table
        String fileName = ResourceUtil.getProperty(appRb, "zonal.regional.auto.flows") + year + ".csv";
        logger.info("  Writing trip table to " + fileName);
        PrintWriter pw = neldtUtil.openFileForSequentialWriting(fileName);
        pw.println("OrigZone,DestZone,ldt_auto,ldt_auto_business,ldt_auto_personal,ldt_auto_commute");
        float[][] prodAttr = new float[2][ncstmUtil.getHighestVal(zones) + 1];

        for (int i: zones) {
            for (int j: zones) {
                float tripsB = allTrips[0].getValueAt(i, j);
                float tripsP = allTrips[1].getValueAt(i, j);
                float tripsC = allTrips[2].getValueAt(i, j);
                float trips = tripsB + tripsP + tripsC;

                // include O-D pairs with trips and all intrazonal O-D pairs to ensure that TransCAD matrix will have all zones included
                if (trips >= 0.00001 || i == j) {
                    pw.format ("%d,%d,%.5f,%.5f,%.5f,%.5f", i, j, trips, tripsB, tripsP, tripsC);
                    pw.println();
                    prodAttr[0][i] += trips;
                    prodAttr[1][j] += trips;
                    tcMatrix[0].setValueAt(i, j, tcMatrix[0].getValueAt(i, j) + tripsB);
                    tcMatrix[0].setValueAt(j, i, tcMatrix[0].getValueAt(j, i) + tripsB);
                    tcMatrix[1].setValueAt(i, j, tcMatrix[1].getValueAt(i, j) + tripsP);
                    tcMatrix[1].setValueAt(j, i, tcMatrix[1].getValueAt(j, i) + tripsP);
                    tcMatrix[2].setValueAt(i, j, tcMatrix[2].getValueAt(i, j) + tripsC);
                    tcMatrix[2].setValueAt(j, i, tcMatrix[2].getValueAt(j, i) + tripsC);
                }
            }
        }
        pw.close();

        String[] purpNames = {neldtPurposes.business.toString(), neldtPurposes.personal.toString(), neldtPurposes.commute.toString()};
        String fileNameTC = ResourceUtil.getProperty(appRb, "zonal.regional.auto.flows") + year + ".mtx";
        writeTrucksToTranscadMatrix(purpNames, tcMatrix, fileNameTC);

        if (ResourceUtil.getBooleanProperty(appRb, "write.auto.prod.attr.by.zone")) {
            String sumFileName = appRb.getString("auto.prod.attr.summary") + "_" + year + ".csv";
            PrintWriter pws = neldtUtil.openFileForSequentialWriting(sumFileName);
            pws.println("Zone,ldAutoProduction,ldAutoAttraction");
            for (int i: zones) {
                if ((prodAttr[0][i] + prodAttr[1][i]) > 0) {
                    pws.println(i + "," + prodAttr[0][i] + "," + prodAttr[1][i]);
                }
            }
            pws.close();
        }
        if (ResourceUtil.getBooleanProperty(appRb, "write.trip.table.by.county", false)) writeCountyTripTable(year, allTrips);
    }


    private HashMap<String, Boolean> findStatesToApplyR3Logit(neldtDisagg nd) {
        // Define for which states R3Logit is applied (mostly states east of the Mississippi

        HashMap<String, Boolean> applyR3Logit = new HashMap<>();
        for (String state: neldt.stateRowList) {
            if (state.equals("Other")) break;  // do not walk through all islands outside the US
            if (state.equals("HI") || state.equals("Caribbean") || state.equals("Europe")) {
                applyR3Logit.put(state, false);
            } else {
                int[] zones = nd.getZonesInState(state);
                applyR3Logit.put(state, ncstmData.applyR3logit(zones[0]));
            }
        }
        return applyR3Logit;
    }


    private Matrix[] addReturnTripToMatrix(neldtPurposes purp, Matrix[] allTrips, int i, int j, float flows) {
        // add round trip to trip matrix

        int order = -1;
        if (purp == neldtPurposes.business) order = 0;
        else if (purp == neldtPurposes.personal) order = 1;
        else if (purp == neldtPurposes.commute) order = 2;
        float outbound = allTrips[order].getValueAt(i, j);
        allTrips[order].setValueAt(i, j, outbound + flows);
        float returnTrp = allTrips[order].getValueAt(j, i);
        allTrips[order].setValueAt(j, i, returnTrp + flows);      // transpose trips to convert tours to trips
        return allTrips;
    }


    private TableDataSet calculateAttractionProductionWeights () {
        // calculate prodAttrWeights for trip production and trip attraction

        // following prodAttrWeights are given in this order: Population, university enrollment, total employment, leisure employment
        TableDataSet prodAttrWeights;
        double[] wghtsPPer = ResourceUtil.getDoubleArray(appRb, "weights.prod.person");
        double[] wghtsAPer = ResourceUtil.getDoubleArray(appRb, "weights.attr.person");
        double[] wghtsPBus = ResourceUtil.getDoubleArray(appRb, "weights.prod.business");
        double[] wghtsABus = ResourceUtil.getDoubleArray(appRb, "weights.attr.business");
        double[] wghtsPCom = ResourceUtil.getDoubleArray(appRb, "weights.prod.commute");
        double[] wghtsACom = ResourceUtil.getDoubleArray(appRb, "weights.attr.commute");
        int[] zones = ncstmData.getZones();
        float[] prodsPer = new float[zones.length];
        float[] attrsPer = new float[zones.length];
        float[] prodsBus = new float[zones.length];
        float[] attrsBus = new float[zones.length];
        float[] prodsCom = new float[zones.length];
        float[] attrsCom = new float[zones.length];
        for (int i = 0; i < zones.length; i++) {
            float[] seDataItems = new float[wghtsPPer.length];
            seDataItems[0]  = ncstmData.getSEdataItem("POP", zones[i]);
            seDataItems[1]  = ncstmData.getSEdataItem("TOTEMP", zones[i]);
            seDataItems[2]  = ncstmData.getSEdataItem("IND", zones[i]);
            seDataItems[3]  = ncstmData.getSEdataItem("HI_IND", zones[i]);
            seDataItems[4]  = ncstmData.getSEdataItem("RET", zones[i]);
            seDataItems[5]  = ncstmData.getSEdataItem("HI_RET", zones[i]);
            seDataItems[6]  = ncstmData.getSEdataItem("OFF", zones[i]);
            seDataItems[7]  = ncstmData.getSEdataItem("SERV", zones[i]);
            seDataItems[8]  = ncstmData.getSEdataItem("GOV", zones[i]);
            seDataItems[9]  = ncstmData.getSEdataItem("EDU", zones[i]);
            seDataItems[10] = ncstmData.getSEdataItem("HOSP", zones[i]);
            seDataItems[11] = ncstmData.getHotelRooms(zones[i]);      // hotelRooms
            seDataItems[12] = ncstmData.getHospitalBeds(zones[i]);    // hospitalBeds
            seDataItems[13] = ncstmData.getParkVisitors(zones[i]);    // park visitors
            seDataItems[14] = ncstmData.getBeachRelevance(zones[i]);  // beaches
            for (int item = 0; item < seDataItems.length; item++) {
                prodsPer[i] += seDataItems[item] * wghtsPPer[item];
                attrsPer[i] += seDataItems[item] * wghtsAPer[item];
                prodsBus[i] += seDataItems[item] * wghtsPBus[item];
                attrsBus[i] += seDataItems[item] * wghtsABus[item];
                prodsCom[i] += seDataItems[item] * wghtsPCom[item];
                attrsCom[i] += seDataItems[item] * wghtsACom[item];
            }
        }
        prodAttrWeights = new TableDataSet();
        prodAttrWeights.appendColumn(zones, "zones");
        prodAttrWeights.appendColumn(prodsPer, "prods" + neldtPurposes.personal.toString());
        prodAttrWeights.appendColumn(attrsPer, "attrs" + neldtPurposes.personal.toString());
        prodAttrWeights.appendColumn(prodsBus, "prods" + neldtPurposes.business.toString());
        prodAttrWeights.appendColumn(attrsBus, "attrs" + neldtPurposes.business.toString());
        prodAttrWeights.appendColumn(prodsCom, "prods" + neldtPurposes.commute.toString());
        prodAttrWeights.appendColumn(attrsCom, "attrs" + neldtPurposes.commute.toString());
        prodAttrWeights.buildIndex(prodAttrWeights.getColumnPosition("zones"));  //cout "hello world"

        // print for testing
//        try {
//            CSVFileWriter writer = new CSVFileWriter();
//            writer.writeFile(prodAttrWeights, new File("/output/weights.csv"));
//        } catch (Exception e) {
//        	logger.error("Error writing to file : /output/weights.csv");
//        	throw new RuntimeException(e);
//        }
            return prodAttrWeights;
    }


    public void writeTrucksToTranscadMatrix(String[] modeNames, Matrix[] modes, String outputToken) {
        // write TransCAD matrix

//        logger.info("  Writing TransCAD matrix.");
//        String fileName = ResourceUtil.getProperty(appRb, outputToken);
        // todo: fix TransCAD Writer
//        MatrixWriter writer = ncstmUtil.createTransCADWriter(appRb, fileName);
//        writer.writeMatrices(modeNames,modes);
    }


    private void writeCountyTripTable (int year, Matrix[] allTrips) {
        // summarize SMZ trip table to county-to-county trip table

        logger.info("  Writing out county-to-county trip table for commute trips");

        TableDataSet counties = ncstmUtil.importTable(appRb.getString("county.ID"));
        int[] fipsCodes = counties.getColumnAsInt("COUNTYFIPS");
//        int[] fipsCodeIndex = new int[ncstmUtil.getHighestVal(fipsCodes) + 1];
//        for (int i = 0; i < fipsCodes.length; i++) fipsCodeIndex[fipsCodes[i]] = i;

        int zones[] = ncstmData.getZones();

        Matrix[] fipsTripTable = new Matrix[3];
        for (int p = 0; p < neldtPurposes.values().length; p++) {
            fipsTripTable[p] = new Matrix(fipsCodes.length, fipsCodes.length);
            fipsTripTable[p].setExternalNumbersZeroBased(fipsCodes);
        }

        for (int p = 0; p < neldtPurposes.values().length; p++) {
            Matrix trips = fipsTripTable[p];
            for (int i: zones) {
                int iFips = ncstmData.getFipsOfZone(i);
                for (int j: zones) {
                    int jFips = ncstmData.getFipsOfZone(j);
                    if (iFips < 0 || jFips < 0) continue;   // some states were not disaggregated to counties
                    trips.setValueAt(iFips, jFips, trips.getValueAt(iFips, jFips) + allTrips[p].getValueAt(i, j));
                }
            }
        }

        PrintWriter pw = ncstmUtil.openFileForSequentialWriting(appRb.getString("county.trip.table") + year + ".csv");
        pw.println("orig,dest,commuteTrips");
        for (int iFips: fipsCodes) {
            for (int jFips: fipsCodes) {
                if (fipsTripTable[1].getValueAt(iFips, jFips) > 0) pw.println(iFips + "," + jFips + "," +
                        fipsTripTable[1].getValueAt(iFips, jFips));
            }
        }
        pw.close();
    }
}
