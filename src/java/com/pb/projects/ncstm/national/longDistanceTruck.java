package com.pb.projects.ncstm.national;

import com.pb.common.datafile.TableDataSet;
import com.pb.common.matrix.ColumnVector;
import com.pb.common.matrix.Matrix;
import com.pb.common.matrix.MatrixBalancerRM;
import com.pb.common.matrix.RowVector;
import com.pb.common.util.IndexSort;
import com.pb.common.util.ResourceUtil;
import com.pb.models.processFAF.*;
import com.pb.projects.ncstm.ncstmData;
import com.pb.projects.ncstm.ncstmUtil;
import com.pb.sawdust.util.concurrent.DnCRecursiveAction;
import com.pb.sawdust.calculator.Function1;
import com.pb.sawdust.util.array.ArrayUtil;
import com.pb.sawdust.util.concurrent.ForkJoinPoolFactory;
import com.pb.sawdust.util.concurrent.IteratorAction;
//import jsr166y.ForkJoinPool;
import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ForkJoinPool;

/**
 * Model to simulate national truck flows for the North Carolina Statewide Model (NCSTM)
 * Author: Rolf Moeckel, PB Albuquerque
 * Data: 1 August 2011
 */
public class longDistanceTruck {

    static Logger logger = Logger.getLogger(longDistanceTruck.class);
    private int year;
    private ResourceBundle appRb;
    private TableDataSet specialRegions;
    private int[] specialRegionsToTaz;
    private int[] zoneIndex;
    private int[] countyFips;
    private int[] fipsToZone;
    private HashMap<String, float[][]> cntFlows;
    private HashMap<Integer, int[]> zonesByCounty;
    private boolean[] disaggCounty;
    private disaggregateFlows df;
    private float aawdtFactor;
    private HashMap<String, Float> useHshLocal;
    private HashMap<String, Float> makeHshLocal;
    private String[] industries;
    private double [][] sutTrucks;
    private double [][] mutTrucks;
    private double [][] msaFlowsTons;
    private boolean enableMSA;
    private float minDist;
    private int numberOfMSA = 12;
    private boolean useDistributionCenters;
    private HashMap <Integer, distributionCenters[]> distCentersByZone;
    private float[] shareThroughDCByCommodity;
    private convertTonsToTrucks cttt;

    public longDistanceTruck (ResourceBundle rp, int year) {
        // Constructor
        this.appRb = rp;
        this.year = year;
        enableMSA = false;
    }


    public void run () {
        // Run method for long-distance truck model

        logger.info("Started long-distance truck model for year " + year);

        readFAF3 faf3 = new readFAF3();
        faf3.readAllData(appRb, year, "tons");
        readTruckData();
        df = new disaggregateFlows();
        df.getUScountyEmploymentByIndustry(appRb);
        createSpecialRegions();
        if (ResourceUtil.getBooleanProperty(appRb, "read.in.raw.faf.data", true)) extractDataByMode(faf3);
        String truckTypeDefinition = "sut_mut";
        df.defineTruckTypes(truckTypeDefinition, appRb);
        disaggregateFromFafToCounties();
        cttt = new convertTonsToTrucks(appRb);
        cttt.readData();
        if (ResourceUtil.getBooleanProperty(appRb, "write.cnty.to.cnty.truck.trps")) writeCountyToCountyTruckTrips();
        disaggregateFromCountiesToZones();
        addEmptyTrucks();
        writeTruckTripTable();
        logger.info("Completed long-distance truck model");
    }


    private void readTruckData () {
        // read general truck data used by several modules

        aawdtFactor = (float) ResourceUtil.getDoubleProperty(appRb, "AADT.to.AAWDT.factor");
        String useToken = "faf3.use.coefficients.local";
        String makeToken = "faf3.make.coefficients.local";
        useHshLocal = disaggregateFlows.createMakeUseHashMap(appRb, useToken);
        makeHshLocal = disaggregateFlows.createMakeUseHashMap(appRb, makeToken);
        TableDataSet useCoeff = fafUtils.importTable(appRb.getString(useToken));
        industries = useCoeff.getColumnAsString("Industry");
        fafUtils.setResourceBundle(appRb);
        minDist = (float) ResourceUtil.getDoubleProperty(appRb, "ignore.flows.below.miles");
        useDistributionCenters = ResourceUtil.getBooleanProperty(appRb, "send.flows.through.dist.cntrs", false);
        if (useDistributionCenters) {
            generateDistrCentersAndIntermodFacilities(appRb.getString("dist.cent.intermod.facilities"));
            readCommoditiesSentThroughDistributionCenters(appRb.getString("commodities.sent.through.dc"));
        }
        int[] zones = ncstmData.getZones();
        zoneIndex = new int[ncstmUtil.getHighestVal(zones)+1];
        for (int i = 0; i < zones.length; i++) {
            zoneIndex[zones[i]] = i;
        }

    }


    private void generateDistrCentersAndIntermodFacilities (String fileToken) {
        // Read file with distribution centers and intermodal facilities

        logger.info("  Reading distribution centers");
        TableDataSet facilities = ncstmUtil.importTable(fileToken);
        for (int row = 1; row <= facilities.getRowCount(); row++) {
            String type = facilities.getStringValueAt(row, "Type");
            if (type.equals("DC") || type.equals("Other: mini-DC") || type.equals("Warehouse")) {
                int id = (int) facilities.getValueAt(row, "ID");
                int taz = (int) facilities.getValueAt(row, "ncstmTAZ");
                float size = facilities.getValueAt(row, "SqFt_calc");
                int fafZone = (int) facilities.getValueAt(row, "fafZone");
                new distributionCenters(id, taz, size, fafZone);      // add distribution center to distributionCenter class
            }
        }
        distCentersByZone = new HashMap<>();
        for (int zone: ncstmData.getZones()) {
            // skip all non-NC zones
            if (ncstmData.getFipsOfZone(zone) < 37000 || ncstmData.getFipsOfZone(zone) > 38000) continue;
            for (distributionCenters dc: distributionCenters.getDistributionCenterArray()) {
                if (ncstmData.getTruckDistance(zone, dc.getTaz()) <= 50) addDistributionCenterToHashMap(zone, dc);
            }
            int foundSites = 0;
            if (distCentersByZone.containsKey(zone)) foundSites = distCentersByZone.get(zone).length;
            if (foundSites < 5) {
                // Could not find distribution centers within 50 miles, add five centers that are further away
                distributionCenters[] allDistributionCenters = distributionCenters.getDistributionCenterArray();
                int[] dist = new int[allDistributionCenters.length];
                for (int dc = 0; dc < allDistributionCenters.length; dc++) {
                    dist[dc] = (int) (ncstmData.getTruckDistance(zone, allDistributionCenters[dc].getTaz()) * 100);
                }
                int[] indexDist = IndexSort.indexSort(dist);
                for (int dc = 0; dc < 5 - foundSites; dc++) {
                    addDistributionCenterToHashMap(zone, allDistributionCenters[indexDist[dc]]);
                }
            }
        }
    }


    private void readCommoditiesSentThroughDistributionCenters (String fileName) {
        // read commodities that are sent through distribution centers

        shareThroughDCByCommodity = new float[ncstmUtil.getHighestVal(readFAF3.sctgCommodities) + 1];
        TableDataSet com = ncstmUtil.importTable(fileName);
        for (int row = 1; row <= com.getRowCount(); row++) {
            String c = com.getStringValueAt(row, "SCTG");
            int cm = Integer.parseInt(c.substring(4));
            float s = com.getValueAt(row, "shareThroughDistCenters");
            shareThroughDCByCommodity[cm] = s;
        }
    }


    private void addDistributionCenterToHashMap (int zone, distributionCenters dc) {
        // add dc to HashMap of distribution centers for this zone

        if (distCentersByZone.containsKey(zone)) {
            distributionCenters[] exist = distCentersByZone.get(zone);
            distributionCenters[] expanded = new distributionCenters[exist.length + 1];
            System.arraycopy(exist, 0, expanded, 0, exist.length);
            expanded[expanded.length - 1] = dc;
            distCentersByZone.put(zone, expanded);
        } else {
            distributionCenters[] setup = {dc};
            distCentersByZone.put(zone, setup);
        }
    }


    private void createSpecialRegions() {
        // Create TableDataSet with specialRegions that are treated specifically. Placeholder for now. Might not be used at all in this project.

        String[] specRegNames = ResourceUtil.getArray(appRb, "special.regions.names");
        String[] specRegModes = ResourceUtil.getArray(appRb, "special.regions.mode");
        int[] specRegFAFCodes = ResourceUtil.getIntegerArray(appRb, "special.regions.faf.codes");
        int[] specRegCodes = ResourceUtil.getIntegerArray(appRb, "special.regions.codes");
        int[] specRegZones = ResourceUtil.getIntegerArray(appRb, "special.regions.zones");
        specialRegions = fafUtils.createSpecialRegions(specRegNames, specRegModes, specRegCodes, specRegZones, specRegFAFCodes);
        specialRegionsToTaz = new int[ncstmUtil.getHighestVal(specRegCodes) + 1];
        for (int i = 0; i < specRegCodes.length; i++) specialRegionsToTaz[specRegCodes[i]] = specRegZones[i];
        countyFips = fafUtils.createCountyFipsArray(specialRegions.getColumnAsInt("modelCode"));
    }


    private void extractDataByMode(readFAF3 faf3) {
        // extract required data

        logger.info("Extracting FAF3 data for modes truck, rail, water and air");
        HashMap<String, Float> scaler = getScaler();
        String truckFileName = ResourceUtil.getProperty(appRb, "temp.truck.flows.faf.zones") + "_" + year;
        faf3.writeFlowsByModeAndCommodity(truckFileName, modesFAF3.Truck, reportFormat.internatOrigToBorderToDest, scaler);
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


    private void disaggregateFromFafToCounties() {
        // disaggregates freight flows from FAF zoneArray to counties

        df.prepareCountyDataForFAFwithDetailedEmployment(appRb, year, specialRegions);
        logger.info("  Disaggregating FAF3 data from FAF zones to counties for year " + year + ".");

        cntFlows = new HashMap<>();

        String[] commodities;
        commodities = readFAF3.sctgStringCommodities;
        int matrixSize = countyFips.length + specialRegions.getRowCount() + 2;  // +2 to add cells for international zones Canada and Mexico
        for (String com: commodities) {
            float[][] dummy = new float[matrixSize][matrixSize];
            //cntFlows matrices: Position 0 to [countyFips.length-1]: counties
            //                   Position [countyFips.length-1] to [countyFips.length+specialRegions.getRowCount-1]: special regions (port of entry)
            //                   Position [countyFips.length+specialRegions.getRowCount-1] to [matrixSize-1]: Canada and Mexico
            cntFlows.put(com, dummy);
        }
        float globalScale = (float) ResourceUtil.getDoubleProperty(appRb, "overall.scaling.factor.truck");

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
                String sctg = readFAF3.getSCTGname(cm);
                float[][] values = cntFlows.get(sctg);
                TableDataSet tblFlows = fafUtils.importTable(fileName);
                for (int row = 1; row <= tblFlows.getRowCount(); row++) {
                    float shortTons = tblFlows.getValueAt(row, "shortTons");
                    if (shortTons == 0) continue;
                    String dir = tblFlows.getStringValueAt(row, "flowDirection");
                    int orig = (int) tblFlows.getValueAt(row, "originFAF");
                    int dest = (int) tblFlows.getValueAt(row, "destinationFAF");
                    if (dir.equals("import")) {
                        if (orig > 800) dest = translateSpecialRegions(dest);
                        else orig = translateSpecialRegions(orig);
                    }
                    if (dir.equals("export")) {
                        if (dest > 800) orig = translateSpecialRegions(orig);
                        else dest = translateSpecialRegions(dest);
                    }
                    TableDataSet singleFlow = df.disaggregateSingleFAF3flow(orig, dest, sctg, shortTons, 1);
                    for (int i = 1; i <= singleFlow.getRowCount(); i++) {
                        int oFips;
                        if (orig < 800 || orig > 900) oFips = getCountyId((int) singleFlow.getValueAt(i, "oFips"));  // domestic county or special region
                        else oFips = countyFips.length + specialRegions.getRowCount() -1 + orig - 800;  // Canada = 801, Mexico = 802
                        int dFips;
                        if (dest < 800 || dest > 900) dFips = getCountyId((int) singleFlow.getValueAt(i, "dFips"));  // domestic county or special region
                        else dFips = countyFips.length + specialRegions.getRowCount() -1 + dest - 800;  // Canada = 801, Mexico = 802
                        float thisFlow = singleFlow.getValueAt(i, "Tons") * globalScale;
                        values[oFips][dFips] += thisFlow;
                    }
                }
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


    private int translateSpecialRegions (int fafZone) {
        // if border/rail yard/port/airport shall be preserved as special region, translate fafZone into modelCode

        int region = fafZone;
        for (int row = 1; row <= specialRegions.getRowCount(); row++)
            if (specialRegions.getValueAt(row, "faf3code") == fafZone)
                region = (int) specialRegions.getValueAt(row, "modelCode");
        return region;
    }


    private int getCountyId(int fips) {
        // Return region code of regName
        for (int i = 0; i < countyFips.length; i++) {
            if (countyFips[i] == fips) return i;
        }
        for (int row = 1; row <= specialRegions.getRowCount(); row ++)
            if (specialRegions.getValueAt(row, "modelCode") == fips) return row + countyFips.length - 1;
        logger.error("Could not find county FIPS code " + fips);
        return -1;
    }


    private void writeCountyToCountyTruckTrips() {
        // optional step: convert county-to-county commodity flows and convert into truck trips

        logger.info("Writing out county-to-county truck trip table");
        float[][][] trucks = new float[2][countyFips.length][countyFips.length];
        for (String com: readFAF3.sctgStringCommodities) {
            float[][] countyFlows = cntFlows.get(com);
            for (int origCnt = 0; origCnt < countyFips.length; origCnt++) {
                int origFips = countyFips[origCnt];
                if (ignoreThisFips(origFips)) continue;  // ignore Guam, Puerto Rico and other distant islands
                for (int destCnt = 0; destCnt < countyFips.length; destCnt++) {
                    int destFips = countyFips[destCnt];
                    if (ignoreThisFips(destFips)) continue;  // ignore Guam, Puerto Rico and other distant islands
                    float distance = df.getCountyDistance(origFips, destFips);
                    double[] trucksByType = cttt.convertThisFlowFromTonsToTrucks(com, distance, (double) countyFlows[origCnt][destCnt]);
                    trucks[0][origCnt][destCnt] += trucksByType[0] / 365.25 * aawdtFactor;
                    trucks[1][origCnt][destCnt] += trucksByType[1];
                }
            }
        }

        PrintWriter pw = fafUtils.openFileForSequentialWriting(appRb.getString("cnty.to.cnty.truck.trip.table") + year + ".csv");
        pw.println("origFips,destFips,singleUnitTrucks,multiUnitTrucks");
        for (int origCnt = 0; origCnt < countyFips.length; origCnt++) {
            int origFips = countyFips[origCnt];
            if (ignoreThisFips(origFips)) continue;  // ignore Guam, Puerto Rico and other distant islands
            for (int destCnt = 0; destCnt < countyFips.length; destCnt++) {
                int destFips = countyFips[destCnt];
                if (ignoreThisFips(destFips)) continue;  // ignore Guam, Puerto Rico and other distant islands
                if (trucks[0][origCnt][destCnt] + trucks[1][origCnt][destCnt] > 0) pw.println(origFips + "," + destFips +
                        "," + trucks[0][origCnt][destCnt] + "," + trucks[1][origCnt][destCnt]);
            }
        }
        pw.close();
    }


    private void disaggregateFromCountiesToZones () {
        // disaggregate truck cntFlows from county-to-county to zone-to-zone flows

        logger.info("Disaggregating truck flows from county-to-county to zone-to-zone flows for " + year);
        defineCountiesToDisaggregate();

        int s = ncstmData.getZones().length;
        sutTrucks = new double[s][s];
        mutTrucks = new double[s][s];
        msaFlowsTons = new double[numberOfMSA + 1][numberOfMSA + 1];

//        final HashMap<String, double[]> localWeights = prepareLocalZonalWeights();
//      logger.info("Disaggregating air-to-truck, rail-to-truck and water-to-truck flows for " + year);
//      for (String com: readFAF3.sctgStringCommodities) {
//            addTrucksServingIntermodalFacility("rail", com, localWeights);
//            addTrucksServingIntermodalFacility("water", com, localWeights);
//            addTrucksServingIntermodalFacility("air", com, localWeights);
//        }

        // or use multi-threaded approach
//        Function1<String,Void> addTrucksServingIntermodalFacilitiesFunction = new Function1<String,Void>() {
//            public Void apply(String commodityModeCombination) {
//                processTrucksThroughFacility(commodityModeCombination, localWeights, distCntTrks, distCntTons, sluTrucks, mutTrucks, maxIdDistCenter);
//                processTrucksThroughFacility(commodityModeCombination, localWeights, distCntTrks, distCntTons, sluTrucks, mutTrucks, maxIdDistCenter);
//                processTrucksThroughFacility(commodityModeCombination, localWeights, distCntTrks, distCntTons, sluTrucks, mutTrucks, maxIdDistCenter);
//                return null;
//            }
//        };
//        int comCount = readFAF3.sctgCommodities.length;
//        String[] cases = new String[comCount * 3];
//        for (int icom = 0; icom < comCount; icom++) {
//            cases[icom] = readFAF3.sctgStringCommodities[icom] + "_rail";
//            cases[icom + comCount] = readFAF3.sctgStringCommodities[icom] + "_water";
//            cases[icom + 2 * comCount] = readFAF3.sctgStringCommodities[icom] + "_air";
//        }
//        Iterator<String> commodityIterator = ArrayUtil.getIterator(cases);
//        IteratorAction<String> facilityTask = new IteratorAction<>(commodityIterator, addTrucksServingIntermodalFacilitiesFunction);
//        ForkJoinPool facilityPool = ForkJoinPoolFactory.getForkJoinPool();
//        facilityPool.execute(facilityTask);
//        facilityTask.waitForCompletion();

        final HashMap<String, double[]> weights = prepareZonalWeights();

        Function1<String,Void> commodityDisaggregationFunctionFAF = new Function1<String,Void>() {
            public Void apply(String com) {
                processCommodityDisaggregationWithDC(com, weights);
//                processCommodityDisaggregation(com, weights);
                return null;
            }
        };

        Iterator<String> commodityIterator = ArrayUtil.getIterator(readFAF3.sctgStringCommodities);
        IteratorAction<String> itTask = new IteratorAction<>(commodityIterator, commodityDisaggregationFunctionFAF);
        ForkJoinPool pool = ForkJoinPoolFactory.getForkJoinPool();
        pool.execute(itTask);
        itTask.waitForCompletion();
    }


    private void defineCountiesToDisaggregate() {
        // select counties that need to be disaggregated

        // walk through every county
        TableDataSet countiesToDisaggregate = fafUtils.importTable(appRb.getString("counties.to.disaggregate"));
        int highestFipsCode = ncstmUtil.getHighestVal(countyFips);
        disaggCounty = new boolean[highestFipsCode + 1];
        fipsToZone = new int[highestFipsCode + 1];
        for (int row = 1; row <= countiesToDisaggregate.getRowCount(); row++) {
            int fips = (int) countiesToDisaggregate.getValueAt(row, "COUNTY");
            disaggCounty[fips] = countiesToDisaggregate.getValueAt(row, "DISAGGNCST") != -1;
            fipsToZone[(int) countiesToDisaggregate.getValueAt(row, "COUNTY")] =
                    (int) countiesToDisaggregate.getValueAt(row, "NCSTMZONE");
        }
        for (int row = 1; row <= specialRegions.getRowCount(); row++) {
            disaggCounty[(int) specialRegions.getValueAt(row, "modelCode")] = false;
        }

        // walk through every zone
        zonesByCounty = new HashMap<>();
        // identify zoneArray by county FIPS
        int[] zones = ncstmData.getZones();
        for (int zone: zones) {
            int fips = ncstmData.getFipsOfZone(zone);
            if (fips == -1) continue;   // outside of area to be disaggregated
            if (disaggCounty[fips]) {
                if (zonesByCounty.containsKey(fips)) {
                    int[] existing = zonesByCounty.get(fips);
                    int[] newList = new int[existing.length + 1];
                    System.arraycopy(existing, 0, newList, 0, existing.length);
                    newList[newList.length - 1] = zone;
                    zonesByCounty.put(fips, newList);
                } else {
                    zonesByCounty.put(fips, new int[] {zone});
                }
            }
        }

    }


    private HashMap<String, double[]> prepareZonalWeights() {
        // prepare zonal weights based on employment by industry

        HashMap<String, double[]> weights = new HashMap<>();

        String[] commodities = readFAF3.sctgStringCommodities;
        for (int fips : countyFips) {
            if (!disaggCounty[fips]) continue;
            int[] zonesInThisCounty = zonesByCounty.get(fips);
//            if (zonesInThisCounty == null) logger.warn ("Check error: " + fips);
            for (String com : commodities) {
                double sumA = 0;
                double sumB = 0;
                double[] makeWeight = new double[zonesInThisCounty.length];
                double[] useWeight = new double[zonesInThisCounty.length];
                for (int iz = 0; iz < zonesInThisCounty.length; iz++) {
                    int zn = zonesInThisCounty[iz];
                    for (String ind : industries) {
                        String code = ind + "_" + com;
                        makeWeight[iz] += ncstmData.getSEdataItem(ind, zn) * makeHshLocal.get(code);
                        useWeight[iz] += ncstmData.getSEdataItem(ind, zn) * useHshLocal.get(code);
                        sumA += makeWeight[iz];
                        sumB += useWeight[iz];
                    }
                }
                // in rare cases where necessary employment for this commodity is not available in this county, use total zonal employment as weights
                // this also applies to counties outside of AZ that are subdivided in smaller zones, for which only total employment is available
                if (sumA == 0) for (int i = 0; i < zonesInThisCounty.length; i++)
                    makeWeight[i] = ncstmData.getSEdataItem("TOTEMP", zonesInThisCounty[i]);
                if (sumB == 0) for (int i = 0; i < zonesInThisCounty.length; i++)
                    useWeight[i] = ncstmData.getSEdataItem("TOTEMP", zonesInThisCounty[i]);
                String mcode = fips + "_" + com + "_make";
                weights.put(mcode, makeWeight);
                String ucode = fips + "_" + com + "_use";
                weights.put(ucode, useWeight);
            }
        }
        return weights;
    }


    private void processCommodityDisaggregationWithDC(String com, Map<String,double[]> weights) {
        // Disaggregate a single commodity from county-to-county flows to zone-to-zone flows
        logger.info("Starting to process " + com);

        int comNum = Integer.parseInt(com.substring(4));
        float[][] countyFlows = cntFlows.get(com);
        int[] zonesOrigCounty;
        int[] zonesDestCounty;
        double[][] msaTonsThisCom = new double[numberOfMSA + 1][numberOfMSA + 1];
        int[] zones = ncstmData.getZones();
        int zoneIndex[] = new int[ncstmUtil.getHighestVal(zones) + 1];
        for (int i = 0; i < zones.length; i++) zoneIndex[zones[i]] = i;

        // create zone array with fips codes, special region codes and Canada & Mexico codes
        int[] zoneArray = new int[countyFips.length + 2];  // + 2 for Canada and Mexico
        System.arraycopy(countyFips, 0, zoneArray, 0, countyFips.length);
        System.arraycopy(new int[]{500002,500003}, 0, zoneArray, countyFips.length, 2);

        for (int origCnt = 0; origCnt < zoneArray.length; origCnt++) {
            int origFips = zoneArray[origCnt];
            if (ignoreThisFips(origFips)) continue;  // ignore Guam, Puerto Rico and other distant islands
            double[] origWeights;
            if (origCnt < countyFips.length - specialRegions.getRowCount()) {       // US counties
                if (disaggCounty[origFips]) {
                    origWeights = weights.get(origFips + "_" + com + "_make");
                    zonesOrigCounty = zonesByCounty.get(origFips);
                } else {
                    origWeights = new double[]{1.};
                    zonesOrigCounty = new int[]{fipsToZone[origFips]};
                }
            } else {                                 // Canada, Mexico or special regions
                origWeights = new double[]{1.};
                zonesOrigCounty = new int[]{zoneArray[origCnt]};
            }
            for (int destCnt = 0; destCnt < zoneArray.length; destCnt++) {
                int destFips = zoneArray[destCnt];
                if (ignoreThisFips(destFips)) continue;  // ignore Guam, Puerto Rico and other distant islands
                float flow = countyFlows[origCnt][destCnt];
                if (flow == 0) continue;
                if (fafUtils.countyFlowConnectsWithHawaii(origFips, destFips)) continue;  // Hawaii is not connected to highway system
                double[] destWeights;
                if (destCnt < countyFips.length - specialRegions.getRowCount()) {   // US counties
                    if (disaggCounty[destFips]) {
                        destWeights = weights.get(destFips + "_" + com + "_use");
                        zonesDestCounty = zonesByCounty.get(destFips);
                    } else {
                        destWeights = new double[]{1.};
                        zonesDestCounty = new int[]{fipsToZone[destFips]};
                    }
                } else {                                 // Canada, Mexico or special regions
                    destWeights = new double[]{1.};
                    zonesDestCounty = new int[]{zoneArray[destCnt]};
                }

                // disaggregate flows from counties to zones
                double[][] disFlow = df.disaggCountyToZones(flow, origWeights, destWeights);

                for (int iz = 0; iz < origWeights.length; iz++) {
                    int oZn = zonesOrigCounty[iz];   // convert to TAZ ID
                    if (oZn == -1) logger.warn("Error 1: " +iz+" "+origFips+" "+disaggCounty[origFips]);
                    if (specialRegionsToTaz[oZn] > 0) oZn = specialRegionsToTaz[oZn];

                    for (int jz = 0; jz < destWeights.length; jz++) {
                        double thisFlow = disFlow[iz][jz];
                        if (thisFlow == 0) continue;
                        int dZn = zonesDestCounty[jz];   // convert to TAZ ID
                        if (dZn == -1) logger.warn("Error 2: " +jz+" "+destFips+" "+disaggCounty[destFips]);
                        if (specialRegionsToTaz[dZn] > 0) dZn = specialRegionsToTaz[dZn];
                        if (oZn == -1) logger.warn("Error 3: " +iz+" "+origFips+" "+disaggCounty[origFips]);
                        if (dZn == -1) logger.warn("Error 4: " +jz+" "+destFips+" "+disaggCounty[destFips]);
                        if (enableMSA && ncstmData.getMSAOfZone(oZn) != -1 && ncstmData.getMSAOfZone(dZn) != -1)
                            msaTonsThisCom[ncstmData.getMSAOfZone(oZn)][ncstmData.getMSAOfZone(dZn)] += thisFlow;

                        float distance = ncstmData.getTruckDistance(oZn, dZn);
                        if (distance < minDist) continue;
                        double dcFlow = 0;
                        if (useDistributionCenters) {
                            dcFlow = shareThroughDCByCommodity[comNum] * thisFlow;
                        }
                        if (dcFlow > 0 && distCentersByZone.containsKey(dZn)) {    // only do for zone in NC for which DC are known
                            distributionCenters[] dc = distCentersByZone.get(dZn);
                            int totSize = 0;
                            for (distributionCenters thisDc: dc) totSize += thisDc.getSize();
                            for (distributionCenters thisDc: dc) {
                                double flowThisDC = dcFlow * thisDc.getSize() / totSize;
                                // trip from origin to distribution center
                                float distanceToDC = ncstmData.getTruckDistance(oZn, thisDc.getTaz());
                                double[] trucksByTypeToDC = cttt.convertThisFlowFromTonsToTrucks(com, distanceToDC, flowThisDC);
                                int oz1 = zoneIndex[oZn];
                                int dct = zoneIndex[thisDc.getTaz()];
                                synchronized (sutTrucks[oz1]) {
                                    sutTrucks[oz1][dct] += trucksByTypeToDC[0] / 365.25 * aawdtFactor;
                                }
                                synchronized (mutTrucks[oz1]) {
                                    mutTrucks[oz1][dct] += trucksByTypeToDC[1] / 365.25 * aawdtFactor;
                                }

                                // trip from distribution center to destination
                                float distanceFromDC = ncstmData.getTruckDistance(thisDc.getTaz(), dZn);
                                double[] trucksByTypeFromDC = cttt.convertThisFlowFromTonsToTrucks(com, distanceFromDC, flowThisDC);
                                int dz2 = zoneIndex[dZn];
                                synchronized (sutTrucks[dct]) {
                                    sutTrucks[dct][dz2] += trucksByTypeFromDC[0] / 365.25 * aawdtFactor;
                                }
                                synchronized (mutTrucks[dct]) {
                                    mutTrucks[dct][dz2] += trucksByTypeFromDC[1] / 365.25 * aawdtFactor;
                                }


                            }
                        }
                        // Remaining flow not going through distribution center
                        thisFlow = thisFlow - dcFlow;
                        if (thisFlow > 0) {
                            double[] trucksByType = cttt.convertThisFlowFromTonsToTrucks(com, distance, thisFlow);
                            int oz = zoneIndex[oZn];
                            int dz = zoneIndex[dZn];
                            synchronized (sutTrucks[oz]) {
                                sutTrucks[oz][dz] += trucksByType[0] / 365.25 * aawdtFactor;
                            }
                            synchronized (mutTrucks[oz]) {
                                mutTrucks[oz][dz] += trucksByType[1] / 365.25 * aawdtFactor;
                            }
                        }
                    }
                }
            }
        }
        if (enableMSA) {
            synchronized (msaFlowsTons[0]) {
                for (int i = 1; i <= numberOfMSA; i++) {
                    for (int j = 1; j <= numberOfMSA; j++) {
                        msaFlowsTons[i][j] += msaTonsThisCom[i][j];
                    }
                }
            }
        }
    }


    private void processCommodityDisaggregation(String com, Map<String,double[]> weights) {
        // Disaggregate a single commodity from county-to-county flows to zone-to-zone flows


        logger.info("  Processing " + com);

        float[][] countyFlows = cntFlows.get(com);
        int[] zonesOrigCounty;
        int[] zonesDestCounty;

        for (int origCnt = 0; origCnt < countyFips.length; origCnt++) {
            int origFips = countyFips[origCnt];
            if (ignoreThisFips(origFips)) continue;  // ignore Guam, Puerto Rico and other distant islands
            double[] origWeights;
            if (zonesByCounty.containsKey(origFips)) {
                origWeights = weights.get(origFips + "_" + com + "_make");
                zonesOrigCounty = zonesByCounty.get(origFips);
            } else {
                origWeights = new double[]{1.};
                zonesOrigCounty = new int[]{fipsToZone[origFips]};
            }

            for (int destCnt = 0; destCnt < countyFips.length; destCnt++) {
                int destFips = countyFips[destCnt];
                if (ignoreThisFips(destFips)) continue;  // ignore Guam, Puerto Rico and other distant islands
                float flow = countyFlows[origCnt][destCnt];
                if (flow == 0) continue;
                if (fafUtils.countyFlowConnectsWithHawaii(origFips, destFips)) continue;  // Hawaii is not connected to highway system
                double[] destWeights;
                if (zonesByCounty.containsKey(destFips)) {
                    destWeights = weights.get(destFips + "_" + com + "_use");
                    zonesDestCounty = zonesByCounty.get(destFips);
                } else {
                    destWeights = new double[]{1.};
                    zonesDestCounty = new int[]{fipsToZone[destFips]};
                }

                // disaggregate flows from counties to zones

                double[][] disFlow = df.disaggCountyToZones(flow, origWeights, destWeights);
                for (int iz = 0; iz < origWeights.length; iz++) {
                    int iTaz = zonesOrigCounty[iz];
                    for (int jz = 0; jz < destWeights.length; jz++) {
                        int jTaz = zonesDestCounty[jz];
                        double thisFlow = disFlow[iz][jz];
                        float distance = ncstmData.getTruckDistance(iTaz, jTaz);
                        if (thisFlow == 0 || distance < minDist) continue;
                        double trucksByType[] = cttt.convertThisFlowFromTonsToTrucks(com, distance, thisFlow);
                        int oz = zoneIndex[iTaz];
                        int dz = zoneIndex[jTaz];

                        synchronized (sutTrucks[oz]) {
                            sutTrucks[oz][dz] += (trucksByType[0] / 365.25 * aawdtFactor);
                        }
                        synchronized (mutTrucks[oz]) {
                            mutTrucks[oz][dz] += (trucksByType[1] / 365.25 * aawdtFactor);
                        }

                    }
                }
            }
        }
        logger.info("Finished processing " + com + " with " + ncstmUtil.getSum(sutTrucks) + " SUT and " +
        ncstmUtil.getSum(mutTrucks) + " MUT trucks.");
    }


    private boolean ignoreThisFips (int fips) {
        return fips > 56045 && fips < 80000;  // ignore Guam, Puerto Rico, and other outlying island
    }


    private void addEmptyTrucks() {
        // Empty truck model to ensure balanced truck volumes entering and leaving every zone

        double emptyRate = (100f - ResourceUtil.getDoubleProperty(appRb, "empty.truck.rate")) / 100f;

        int[] zones = ncstmData.getZones();
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

        logger.info("Trucks generated by commodity flows: " + Math.round((float) loadedSutTot) + " SUT and " +
                Math.round((float) loadedMutTot) + " MUT.");
        logger.info("Empty trucks generated by balancing: " + Math.round((float) emptySutRetTot) + " SUT and " +
                Math.round((float) emptyMutRetTot) + " MUT.");
        double correctedEmptyTruckRate = emptyRate + (emptySutRetTot + emptyMutRetTot) / (targetSut + targetMut);
        if (correctedEmptyTruckRate < 0) logger.warn("Empty truck rate for returning trucks is with " +
                ncstmUtil.rounder(((emptySutRetTot + emptyMutRetTot) / (targetSut + targetMut)), 2) +
                " greater than global empty-truck rate of " + ncstmUtil.rounder(emptyRate, 2));
        logger.info("Empty trucks added by statistics:    " + Math.round((float) ((1 - correctedEmptyTruckRate) * targetSut)) +
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

        int[] zones = ncstmData.getZones();
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
                emptyTruckOrig.setValueAt(zones[zn], (float) -trucks[zn]);
                emptyTruckDest.setValueAt(zones[zn], 0f);
            }
        }
        Matrix seed = new Matrix(zones.length, zones.length);
        seed.setExternalNumbersZeroBased(zones);
        for (int o: zones) {
            for (int d: zones) {
                float friction = (float) Math.exp(-0.001 * ncstmData.getTruckDistance(o, d));
                seed.setValueAt(o, d, friction);
            }
        }
        MatrixBalancerRM mb = new MatrixBalancerRM(seed, emptyTruckOrig, emptyTruckDest, 0.001, 10, MatrixBalancerRM.ADJUST.BOTH_USING_AVERAGE);
        return mb.balance();
    }


    private void writeTruckTripTable () {
        // Write out truck trip table

        String fileName = appRb.getString("zonal.truck.flows") + "_" + year + ".csv";
        logger.info("Writing truck trips to file " + fileName);
        PrintWriter pw = fafUtils.openFileForSequentialWriting(fileName);
        pw.println("orig,dest,singleUnitTrucks,multiUnitTrucks");
        int[] zones = ncstmData.getZones();
        double[][] ps = new double[2][zones.length];
        double[][] as = new double[2][zones.length];
        for (int zone = 0; zone < zones.length; zone++) sutTrucks[zone][zone] += 0.1;  // inserted to ensure that matrix contains all zones
        for (int orig = 0; orig < zones.length; orig++) {
            for (int dest = 0; dest < zones.length; dest++) {
                // only write lines that have data, and write all intrazonal cells to ensure TransCAD matrix will have all zones included
                if (sutTrucks[orig][dest] + mutTrucks[orig][dest] > 0 || orig == dest) {
                    pw.format ("%d,%d,%.6f,%.6f", zones[orig], zones[dest], sutTrucks[orig][dest],
                            mutTrucks[orig][dest]);
                    pw.println();
                    ps[0][orig] += sutTrucks[orig][dest];
                    ps[1][orig] += mutTrucks[orig][dest];
                    as[0][dest] += sutTrucks[orig][dest];
                    as[1][dest] += mutTrucks[orig][dest];
                }
            }
        }
        pw.close();
        if (ResourceUtil.getBooleanProperty(appRb, "write.truck.prod.attr.by.zone")) {
            String sumFileName = appRb.getString("truck.prod.attr.summary") + "_" + year + ".csv";
            PrintWriter pws = fafUtils.openFileForSequentialWriting(sumFileName);
            pws.println("Zone,sutProduction,sutAttraction,mutProduction,mutAttraction");
            for (int i = 0; i < zones.length; i++) {
                if ((ps[0][i] + ps[1][i] + as[0][i] + as[1][i]) > 0) {
                    pws.println(zones[i] + "," + ps[0][i] + "," + as[0][i] + "," + ps[1][i] + "," + as[1][i]);
                }
            }
            pws.close();
        }
        if (ResourceUtil.getBooleanProperty(appRb, "write.msa.to.msa.ton.flows")) {
            if (enableMSA) {
            String sumFileName = appRb.getString("msa.to.msa.ton.flows") + "_" + year + ".csv";
                PrintWriter pws = fafUtils.openFileForSequentialWriting(sumFileName);
                pws.println("origMSA,destMSA,tons");
                for (int i = 1; i <= numberOfMSA; i++) {
                    for (int j = 1; j <= numberOfMSA; j++) {
                        pws.println(i + "," + j + "," + msaFlowsTons[i][j]);
                    }
                }
                pws.close();
            } else {
                logger.warn("MSA to MSA flows were temporarily disabled. Check code for <boolean enableMSA>");
            }
        }
    }

}
