package com.pb.projects.ncstm.mpoTrucks;

import com.pb.common.datafile.TableDataSet;
import com.pb.common.matrix.ColumnVector;
import com.pb.common.matrix.Matrix;
import com.pb.common.matrix.MatrixBalancerRM;
import com.pb.common.matrix.RowVector;
import com.pb.common.util.ResourceUtil;
import com.pb.models.processFAF.*;
import com.pb.projects.ncstm.ncstmUtil;
import com.pb.sawdust.util.concurrent.DnCRecursiveAction;
import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 * Model to simulate truck flows for the Three MPO Regions in North Carolina: Metrolina, Triad and Triangle
 * Author: Rolf Moeckel, PB Albuquerque
 * Data: 2 October 2012 (Santa Fe)
 */


public class ldTrucks {

    static Logger logger = Logger.getLogger(ldTrucks.class);
    private int year;
    private ResourceBundle appRb;
    private disaggregateFlows df;
//    private HashMap<String, Float> useHshLocal;
//    private HashMap<String, Float> makeHshLocal;
//    private String[] industries;
    private double [][] sutTrucks;
    private double [][] mutTrucks;
    private HashMap<String, float[][]> cntFlows;
    private int[] countyFips;
    private int[] countyFipsIndex;


    public ldTrucks (ResourceBundle rp, int year) {
        // Constructor
        this.appRb = rp;
        this.year = year;
    }


    public void run () {
        // Run method for long-distance truck model

        logger.info("  Started long-distance truck model for year " + year);
        fafUtils.setResourceBundle(appRb);
        readFAF3 faf3 = new readFAF3();
        faf3.readAllData (appRb, year, "tons");
        if (ResourceUtil.getBooleanProperty(appRb, "read.in.raw.faf.data", true)) extractDataByMode(faf3);
        df = new disaggregateFlows();
        df.getUScountyEmploymentByIndustry(appRb);
        String truckTypeDefinition = "sut_mut";
        df.defineTruckTypes(truckTypeDefinition, appRb);
        createZoneList();
        df.prepareCountyDataForFAFwithDetailedEmployment(appRb, year, false);
        df.scaleSelectedCounties(appRb);
        faf3.definePortsOfEntry(appRb);
        disaggregateFromFafToCounties();
        convertFromCommoditiesToTrucks();
        addEmptyTrucks();
        writeOutDisaggregatedTruckTrips();
    }


    private void extractDataByMode(readFAF3 faf3) {
        // extract required data

        logger.info("Extracting FAF3 data for modes truck, rail, water and air");
        HashMap<String, Float> scaler = getScaler();
        String truckFileName = ResourceUtil.getProperty(appRb, "temp.truck.flows.faf.zones") + "_" + year;
        faf3.writeFlowsByModeAndCommodity(truckFileName, modesFAF3.Truck, reportFormat.internat_domesticPart, scaler);
        String railFileName = ResourceUtil.getProperty(appRb, "temp.rail.flows.faf.zones") + "_" + year;
        faf3.writeFlowsByModeAndCommodity(railFileName, modesFAF3.Rail, reportFormat.internatOrigToBorderToDest, scaler);
        String waterFileName = ResourceUtil.getProperty(appRb, "temp.water.flows.faf.zones") + "_" + year;
        faf3.writeFlowsByModeAndCommodity(waterFileName, modesFAF3.Water, reportFormat.internatOrigToBorderToDest, scaler);
        String airFileName = ResourceUtil.getProperty(appRb, "temp.air.flows.faf.zones") + "_" + year;
        faf3.writeFlowsByModeAndCommodity(airFileName, modesFAF3.Air, reportFormat.internatOrigToBorderToDest, scaler);
    }


    private HashMap<String,Float> getScaler() {
        // create HashMap with state O-D pairs that need to be scaled

        HashMap<String, Float> scaler = new HashMap<>();
        String[] tokens = ResourceUtil.getArray(appRb, "scaling.truck.trips.tokens");
        double[] values = ResourceUtil.getDoubleArray(appRb, "scaling.truck.trips.values");
        if (tokens.length != values.length) {
            throw new RuntimeException("Error.  scaling.truck.trips.tokens must be same length as scaling.truck.trips.values");
        }
        for (int i=0; i<tokens.length; i++) {
            scaler.put(tokens[i], (float) values[i]);
        }
        return scaler;
    }


    private void createZoneList() {
        // Create array with specialRegions that serve as port of entry/exit

        TableDataSet poe = fafUtils.importTable(appRb.getString("ports.of.entry"));
        countyFips = fafUtils.createCountyFipsArray(poe.getColumnAsInt("pointOfEntry"));
        countyFipsIndex = new int[fafUtils.getHighestVal(countyFips) + 1];
        for (int i = 0; i < countyFips.length; i++) {
            countyFipsIndex[countyFips[i]] = i;
        }
    }


    private void disaggregateFromFafToCounties() {
        // disaggregates freight flows from FAF zoneArray to counties

        logger.info("  Disaggregating FAF3 data from FAF zones to counties for year " + year + ".");

        cntFlows = new HashMap<>();
        String[] commodities = readFAF3.sctgStringCommodities;
        int matrixSize = countyFips.length;
        cntFlows = new HashMap<>();

        float globalScale = (float) ResourceUtil.getDoubleProperty(appRb, "master.scaling.factor.truck");

        // regular method
        for (String com: commodities) {
            float[][] dummy = new float[matrixSize][matrixSize];
            cntFlows.put(com, dummy);
        }

        java.util.concurrent.ForkJoinPool pool = new java.util.concurrent.ForkJoinPool();
        DnCRecursiveAction action = new DissaggregateFafAction(globalScale);
        pool.execute(action);
        action.getResult();
    }


    private class DissaggregateFafAction extends DnCRecursiveAction {
        private final float globalScale;

        private DissaggregateFafAction(float globalScale) {
            super(0,readFAF3.sctgStringCommodities.length);
            this.globalScale = globalScale;
        }

        private DissaggregateFafAction(float globalScale, long start, long length, DnCRecursiveAction next) {
            super(start,length,next);
            this.globalScale = globalScale;
        }

        @Override
        protected void computeAction(long start, long length) {
            long end = start + length;
            for (int comm = (int) start; comm < end; comm++) {
                int cm = readFAF3.sctgCommodities[comm];
                String fileName = ResourceUtil.getProperty(appRb, "temp.truck.flows.faf.zones") + "_" + year;
                if (cm < 10) fileName = fileName + "_SCTG0" + cm + ".csv";
                else fileName = fileName + "_SCTG" + cm + ".csv";
                logger.info("  - Working on " + fileName);
                float sm = 0;
                String sctg = readFAF3.getSCTGname(cm);
                float[][] values = cntFlows.get(sctg);
                TableDataSet tblFlows = fafUtils.importTable(fileName);
                for (int row = 1; row <= tblFlows.getRowCount(); row++) {
                    float shortTons = tblFlows.getValueAt(row, "shortTons");
                    if (shortTons == 0) continue;
                    String dir = tblFlows.getStringValueAt(row, "flowDirection");
                    int orig = (int) tblFlows.getValueAt(row, "originFAF");
                    int dest = (int) tblFlows.getValueAt(row, "destinationFAF");
                    TableDataSet singleFlow;
                    if (dir.equals("import") || dir.equals("export")) {
                        TableDataSet poe;
                        if (dir.equals("import")) poe = readFAF3.getPortsOfEntry(orig);
                        else poe = readFAF3.getPortsOfEntry(dest);
                        singleFlow = df.disaggregateSingleFAF3flowThroughPOE(dir, poe, orig, dest, sctg, shortTons, 1);
                    } else singleFlow = df.disaggregateSingleFAF3flow(orig, dest, sctg, shortTons, 1);
                    for (int i = 1; i <= singleFlow.getRowCount(); i++) {
                        int oFips = (int) singleFlow.getValueAt(i, "oFips");
                        int oZone = getCountyId(oFips);
                        int dFips = (int) singleFlow.getValueAt(i, "dFips");
                        int dZone = getCountyId(dFips);
                        float thisFlow = singleFlow.getValueAt(i, "Tons") * globalScale;
                        values[oZone][dZone] += thisFlow;
                        sm = sm + thisFlow;
                    }
                }
//                logger.info("     Processed " + sm + " tons");
            }
        }

        @Override
        protected DnCRecursiveAction getNextAction(long start, long length, DnCRecursiveAction next) {
            return new DissaggregateFafAction(globalScale,start,length,next);
        }

        @Override
        protected boolean continueDividing(long length) {
            return getSurplusQueuedTaskCount() < 3 && length > 1;
        }
    }


    private int getCountyId(int fips) {
        // Return region code of regName
        return countyFipsIndex[fips];
    }


    private void convertFromCommoditiesToTrucks() {
        // generate truck flows based on commodity flows

        logger.info("  Converting flows in tons into truck trips");
        float aawdtFactor = (float) ResourceUtil.getDoubleProperty(appRb, "AADT.to.AAWDT.factor");
        double sutMultiplier = ResourceUtil.getDoubleProperty(appRb, "multiplier.SUT.payload");
        double mutMultiplier = ResourceUtil.getDoubleProperty(appRb, "multiplier.MUT.payload");
        float minDist = (float) ResourceUtil.getDoubleProperty(appRb, "ignore.flows.below.miles");
        float adjustmentSUT = (float) ResourceUtil.getDoubleProperty(appRb, "percent.adjustment.sut", 0f);
        sutTrucks = new double[countyFips.length][countyFips.length];
        mutTrucks = new double[countyFips.length][countyFips.length];
        for (String com: readFAF3.sctgStringCommodities) {
            double avPayload = fafUtils.findAveragePayload(com, "SCTG");
            double sutPL = sutMultiplier * avPayload;
            double mutPL = mutMultiplier * avPayload;
            float[][] tonFlows = cntFlows.get(com);
            for (int i: countyFips) {
                for (int j: countyFips) {
                    float dist = df.getCountyDistance(i, j);
                    if (dist < 0 || dist < minDist) continue;  // skip flows to Guam, Puerto Rico, Hawaii, Alaskan Islands etc.
                    int orig = getCountyId(i);
                    int dest = getCountyId(j);
                    if (tonFlows[orig][dest] == 0) continue;
                    double[] trucksByType = df.getTrucksByType(dist, sutPL, mutPL, tonFlows[orig][dest], adjustmentSUT);
                    // Annual cntFlows divided by 365.25 days plus AAWDT-over-AADT factor
                    trucksByType[0] = trucksByType[0] / 365.25f * (1 + (aawdtFactor / 100));
                    trucksByType[1] = trucksByType[1] / 365.25f * (1 + (aawdtFactor / 100));
                    sutTrucks[orig][dest] += trucksByType[0];
                    mutTrucks[orig][dest] += trucksByType[1];
                }
            }
        }
    }


    private void addEmptyTrucks() {
        // Empty truck model to ensure balanced truck volumes entering and leaving every zone

        logger.info("  Adding empty truck trips");
        int[] zones = df.getCountyFips();
        double emptyRate = (100f - ResourceUtil.getDoubleProperty(appRb, "empty.truck.rate")) / 100f;
        double[] balSut = new double[zones.length];
        double[] balMut = new double[zones.length];
        for (int orig = 0; orig < zones.length; orig++) {
            for (int dest = 0; dest < zones.length; dest++) {
                balSut[orig] -= sutTrucks[orig][dest];
                balSut[dest] += sutTrucks[orig][dest];
                balMut[orig] -= mutTrucks[orig][dest];
                balMut[dest] += mutTrucks[orig][dest];
            }
        }
        Matrix emptySut = balanceEmpties(balSut);
        Matrix emptyMut = balanceEmpties(balMut);
        double loadedSutTot = ncstmUtil.getSum(sutTrucks);
        double loadedMutTot = ncstmUtil.getSum(mutTrucks);
        double targetSut = loadedSutTot / emptyRate;
        double targetMut = loadedMutTot / emptyRate;
        double emptySutRetTot = emptySut.getSum();
        double emptyMutRetTot = emptyMut.getSum();

        logger.info("  Trucks generated by commodity flows: " + Math.round((float) loadedSutTot) + " SUT and " +
                Math.round((float) loadedMutTot) + " MUT.");
        logger.info("  Empty trucks generated by balancing: " + Math.round((float) emptySutRetTot) + " SUT and " +
                Math.round((float) emptyMutRetTot) + " MUT.");
        double correctedEmptyTruckRate = emptyRate + (emptySutRetTot + emptyMutRetTot) / (targetSut + targetMut);
        if (correctedEmptyTruckRate < 0) logger.warn("  Empty truck rate for returning trucks is with " +
                ncstmUtil.rounder(((emptySutRetTot + emptyMutRetTot) / (targetSut + targetMut)), 2) +
                " greater than global empty-truck rate of " + ncstmUtil.rounder(emptyRate, 2));
        logger.info("  Empty trucks added by statistics:    " + Math.round((float) ((1 - correctedEmptyTruckRate) * targetSut)) +
                " SUT and " + Math.round((float) ((1 - correctedEmptyTruckRate) * targetMut)) + " MUT.");

        for (int orig = 0; orig < zones.length; orig++) {
            for (int dest = 0; dest < zones.length; dest++) {
                float emptySutReturn = emptySut.getValueAt(zones[dest], zones[orig]);  // note: orig and dest are switched to get return trip
                float emptyMutReturn = emptyMut.getValueAt(zones[dest], zones[orig]);  // note: orig and dest are switched to get return trip
                double emptySutStat = (sutTrucks[orig][dest] + emptySutReturn) / correctedEmptyTruckRate -
                        (sutTrucks[orig][dest] + emptySutReturn);
                double emptyMutStat = (mutTrucks[orig][dest] + emptyMutReturn) / correctedEmptyTruckRate -
                        (mutTrucks[orig][dest] + emptyMutReturn);

                sutTrucks[orig][dest] += emptySutReturn + emptySutStat;
                mutTrucks[orig][dest] += emptyMutReturn + emptyMutStat;
            }
        }
    }


    private Matrix balanceEmpties(double[] trucks) {
        // generate empty truck trips

        int[] zones = df.getCountyFips();
        RowVector emptyTruckDest = new RowVector(zones.length);
        emptyTruckDest.setExternalNumbersZeroBased(zones);
        ColumnVector emptyTruckOrig = new ColumnVector(zones.length);
        emptyTruckOrig.setExternalNumbersZeroBased(zones);
        for (int zn = 0; zn < zones.length; zn++) {
            if (trucks[zn] > 0) {
                emptyTruckDest.setValueAt(zones[zn], (float) trucks[zn]);
                emptyTruckOrig.setValueAt(zones[zn], 0f);
            }
            else {
                emptyTruckOrig.setValueAt(zones[zn], (float) trucks[zn]);
                emptyTruckDest.setValueAt(zones[zn], 0f);
            }
        }
        Matrix seed = new Matrix(zones.length, zones.length);
        seed.setExternalNumbersZeroBased(zones);
        for (int o: zones) {
            for (int d: zones) {
                float friction = (float) Math.exp(-0.001 * df.getCountyDistance(o, d));
                seed.setValueAt(o, d, friction);
            }
        }
        MatrixBalancerRM mb = new MatrixBalancerRM(seed, emptyTruckOrig, emptyTruckDest, 0.001, 10,
                MatrixBalancerRM.ADJUST.BOTH_USING_AVERAGE);
        return mb.balance();
    }


    private void writeOutDisaggregatedTruckTrips() {
        // write out disaggregated truck trips

        String fileName = appRb.getString("county.truck.flows") + "_" + year + ".csv";
        logger.info("  Writing results to file " + fileName);
        PrintWriter pw = fafUtils.openFileForSequentialWriting(fileName);
        pw.println("OrigFips,DestFips,sut,mut");
        for (int i: countyFips) {
            for (int j: countyFips) {
                int orig = getCountyId(i);
                int dest = getCountyId(j);
                double slu = sutTrucks[orig][dest];
                double mtu = mutTrucks[orig][dest];
                if (slu + mtu >= 0.00001) {
                    pw.format ("%d,%d,%.5f,%.5f", i, j, slu, mtu);
                    pw.println();
                }
            }
        }
        pw.close();
    }

}
