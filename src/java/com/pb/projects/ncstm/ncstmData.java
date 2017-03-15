package com.pb.projects.ncstm;

import com.pb.common.datafile.TableDataSet;
import com.pb.common.matrix.Matrix;
import com.pb.common.matrix.MatrixReader;
import com.pb.common.matrix.MatrixType;
import com.pb.common.util.ResourceUtil;
import com.pb.projects.ncstm.ncstmUtil;
import com.pb.projects.ncstm.mpoTrucks.producePAs;

import java.io.File;
import java.util.ResourceBundle;

/**
 * Data set for model to simulate truck flows for the North Carolina Statewide Model (NCSTM)
 * Author: Rolf Moeckel, PB Albuquerque
 * Data: 29 July 2011
 */
public class ncstmData {

    static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ncstmData.class);
    private static TableDataSet zonalSystem,prodAndAttr,tripRates,lnTripRates,seData,hotelRooms,hospitals,parkVisitors;
    private static String personLongDistanceTravelMonth;
    private static Matrix truckDistance;
    private static Matrix autoTime;
    private static Matrix autoDistance;
    private static int numberOfZones;


    public static void readZonalData (ResourceBundle rb, int year) {
        // read zonal data
        zonalSystem = ncstmUtil.importTable(rb.getString("zonal.system"));
        zonalSystem.buildIndex(zonalSystem.getColumnPosition("TAZ"));

        seData = ncstmUtil.importTable(rb.getString("socio.economic.data.file"));

        // Add households by income, which is used by the commercial vehicle model trip rates
        TableDataSet hhByInc = ncstmUtil.importTable(rb.getString("socio.economic.hh.by.inc"));
        hhByInc.buildIndex(hhByInc.getColumnPosition("MODEL_TAZ"));
        int[] seZones = seData.getColumnAsInt("MODEL_TAZ");
        float[] inc1 = new float[seZones.length];
        float[] inc2 = new float[seZones.length];
        float[] inc3 = new float[seZones.length];
        float[] inc4 = new float[seZones.length];
        float[] inc5 = new float[seZones.length];

        for (int i = 0; i < seZones.length; i++) {
            inc1[i] = hhByInc.getIndexedValueAt(seZones[i], "INC1");
            inc2[i] = hhByInc.getIndexedValueAt(seZones[i], "INC2");
            inc3[i] = hhByInc.getIndexedValueAt(seZones[i], "INC3");
            inc4[i] = hhByInc.getIndexedValueAt(seZones[i], "INC4");
            inc5[i] = hhByInc.getIndexedValueAt(seZones[i], "INC5");
        }

        seData.appendColumn(inc1, "INC1");
        seData.appendColumn(inc2, "INC2");
        seData.appendColumn(inc3, "INC3");
        seData.appendColumn(inc4, "INC4");
        seData.appendColumn(inc5, "INC5");

        // build index on model TAZ number
        seData.buildIndex(seData.getColumnPosition("MODEL_TAZ"));

        String fileName = rb.getString("zonal.system");
        zonalSystem = ncstmUtil.importTable(fileName);    
      
        //building index on the TAZ column
        zonalSystem.buildIndex(zonalSystem.getColumnPosition("TAZ"));
        
        personLongDistanceTravelMonth = rb.getString("month.modeled");

        hotelRooms = ncstmUtil.importTable(rb.getString("hotel.rooms.vacation.rentals"));
        hotelRooms.buildIndex(hotelRooms.getColumnPosition("Model_SMZ"));

        hospitals = ncstmUtil.importTable(rb.getString("hospitals"));
        hospitals.buildIndex(hospitals.getColumnPosition("TAZ_NCSTM"));

        parkVisitors = ncstmUtil.importTable(rb.getString("park.visitors"));
        parkVisitors.buildIndex(parkVisitors.getColumnPosition("NCSTM"));

        numberOfZones = zonalSystem.getRowCount();
//        logger.info("MaxTAZ: "+ numberOfZones);
    }


    public static int[] getZones () {
        // return int array with zone IDs
        return zonalSystem.getColumnAsInt("TAZ");
    }

    public static boolean applyR3logit (int taz) {
        // Find out if mode split is modeled for this zone (applies mostly to zones east of the Mississippi

        for (int row = 1; row <= zonalSystem.getRowCount(); row++) {
            if (zonalSystem.getValueAt(row, "TAZ") == taz) {
                return zonalSystem.getBooleanValueAt(row, "applyR3logit");
            }
        }
        return false;
    }


    public static int getNumberOfZones() {
        return numberOfZones;
    }


    public static boolean zoneInNCSTMArea (int taz) {
        return taz < 200000;// || (taz > 400000 && taz < 500000);
    }


    public static TableDataSet getZoneSystem() {
        // return TableDataSet of zone system
        return zonalSystem;
    }

    public static float getSEdataItem (String dataItem, int zone) {
        // return single zone value with socio-economic data of column dataItem
        return seData.getIndexedValueAt(zone, dataItem);
    }


    public static float getHotelRooms (int taz) {
        // return calibrated hotel room weight
        try {
            float rooms = hotelRooms.getIndexedValueAt(taz, "Hotel_Rms") + hotelRooms.getIndexedValueAt(taz, "Vac_BedRms");
            float calibrator = hotelRooms.getIndexedValueAt(taz, "Adjustment");
            if (!personLongDistanceTravelMonth.equalsIgnoreCase("average")) rooms *=
                    hotelRooms.getIndexedValueAt(taz, personLongDistanceTravelMonth) / 100f;
            return rooms * calibrator;
        } catch (Exception e) {
            return 0;
        }
    }


    public static float getBeachRelevance (int taz) {
        // return 0, 1, 2 or 3 for no beach, Beach low, Beach medium and Beach high
        try {
            return (int) hotelRooms.getIndexedValueAt(taz, "Beach_TAZ");
        } catch (Exception e) {
            return 0;
        }
    }


    public static float getHospitalBeds (int taz) {
        // return calibrated hospital beds weight
        try {
            float beds = hospitals.getIndexedValueAt(taz, "Tot_Beds");
            float shareLongDistance = hospitals.getIndexedValueAt(taz, "shareLongDistance");
            float calibrator = hotelRooms.getIndexedValueAt(taz, "Adjustment");
            return beds * shareLongDistance * calibrator;
        } catch (Exception e) {
            return 0;
        }
    }


    public static float getParkVisitors (int taz) {
        // return calibrated hospital beds weight
        try {
            float visitors;
            if (personLongDistanceTravelMonth.equalsIgnoreCase("average")) {
                visitors = parkVisitors.getIndexedValueAt(taz, "dailyVisitor");
            } else {
                visitors = parkVisitors.getIndexedValueAt(taz, personLongDistanceTravelMonth);
            }
            float calibrator = parkVisitors.getIndexedValueAt(taz, "Adjustment");
            return visitors * calibrator;
        } catch (Exception e) {
            return 0;
        }
    }


    public static int getFipsOfZone (int taz) {
        return (int) zonalSystem.getIndexedValueAt(taz, "Fips");
    }


    public static int getAreaTypeOfZone (int taz) {
        return (int) zonalSystem.getIndexedValueAt(taz, "AREA_TYPE");
    }

    public static int getMSAOfZone (int taz) {
        try {
            return (int) zonalSystem.getIndexedValueAt(taz, "msa");
        } catch (Exception e) {
            logger.warn("Could not find MSA of TAZ " + taz);
            return 0;
        }
    }


    public static void readSkims(ResourceBundle rb, int year) {
        // read skim matrices
        if (rb.getString("format.of.skims").equals("transcad")) {
            logger.info("  Reading TransCAD skim matrices");
            readTransCADMatrix(rb);
        } else if (rb.getString("format.of.skims").equals("zmx")) {
            logger.info("  Reading zip skim matrices");
            readZipMatrices(rb, year);
        } else if (rb.getString("format.of.skims").equals("csv")) {
            logger.info("  Reading csv skim matrices");
            readCsvMatrices(rb, year);
        } else {
            logger.error("Unknown skim format set at format.of.skims: " + rb.getString("format.of.skims") +
                    ". Use transcad, zmx or csv instead.");
        }
        // overwrite intrazonal distance in Atlanta, as many trips there are longer than 50 miles, even though the
        // calculated intrazonal travel distance is under 50 miles
        autoDistance.setValueAt(313011, 313011, 50.1f);
    }


    private static void readTransCADMatrix(ResourceBundle rb){
    	// read in transCad matrices
        // read auto skims
        String fileNameAutos = ResourceUtil.getProperty(rb, "transcad.auto.skim");
    	MatrixReader transcadReaderAutos = MatrixReader.createReader(MatrixType.TRANSCAD, new File(fileNameAutos));
        autoTime = transcadReaderAutos.readMatrix(ResourceUtil.getProperty(rb, "time.matrix.name"));
        autoDistance = transcadReaderAutos.readMatrix(ResourceUtil.getProperty(rb, "distance.matrix.name"));
        // read truck skims
        String fileNameTrucks = ResourceUtil.getProperty(rb, "transcad.truck.skim");
        MatrixReader transcadReaderTrucks = MatrixReader.createReader(MatrixType.TRANSCAD, new File(fileNameTrucks));
        truckDistance = transcadReaderTrucks.readMatrix(ResourceUtil.getProperty(rb, "distance.matrix.name"));
    }


    private static void readZipMatrices(ResourceBundle rb, int year) {
        // read zmx zip matrices
        // read auto skims
        MatrixReader readerAutoTime = MatrixReader.createReader(rb.getString("zmx.auto.time.skim") + year + ".zmx");
        autoTime = readerAutoTime.readMatrix();
        MatrixReader readerAutoDistance = MatrixReader.createReader(rb.getString("zmx.auto.distance.skim") + year + ".zmx");
        autoDistance = readerAutoDistance.readMatrix();
        // read truck skims
        MatrixReader readerTruckDistance = MatrixReader.createReader(rb.getString("zmx.truck.distance.skim") + year + ".zmx");
        truckDistance = readerTruckDistance.readMatrix();
    }


    private static void readCsvMatrices (ResourceBundle rb, int year) {
        // read csv zip matrices

        String fileNameAutoDist = rb.getString("csv.auto.distance.skim") + year + ".csv";
        autoDistance = readSingleCsvMatrix(fileNameAutoDist, "dist");
        String fileNameAutoTime = rb.getString("csv.auto.time.skim") + year + ".csv";
        autoTime = readSingleCsvMatrix(fileNameAutoTime, "time");
        String fileNameTrkDist = rb.getString("csv.truck.distance.skim") + year + ".csv";
        truckDistance = readSingleCsvMatrix(fileNameTrkDist, "dist");
    }


    private static Matrix readSingleCsvMatrix (String fileName, String columnName) {
        // read single csv matrix from fileName

        int[] zones = getZones();
        TableDataSet tblSet = ncstmUtil.importTable(fileName);
        Matrix mat = new Matrix(zones.length, zones.length);
        mat.setExternalNumbersZeroBased(zones);
        for (int row = 1; row < tblSet.getRowCount(); row++) {
            int orig = (int) tblSet.getValueAt(row, "orig");
            int dest = (int) tblSet.getValueAt(row, "dest");
            float dist = tblSet.getValueAt(row, columnName);
            mat.setValueAt(orig, dest, dist);
        }
        return mat;
    }


    public static float getTruckDistance (int i, int j) {
        try {
            return truckDistance.getValueAt(i, j);
        } catch (Exception e) {
            logger.error("No truck distance value from zone " + i + " to zone " + j);
            return -1;
        }
    }


    public static float getAutoTravelTime (int i, int j) {
        try {
            return autoTime.getValueAt(i, j);
        } catch (Exception e) {
            logger.error("No auto travel time value from zone " + i + " to zone " + j);
            return -1;
        }
    }


    public static Matrix getAutoTimeSkim () {
        // return time skim
        return autoTime;
    }


    public static Matrix getAutoDistanceSkim () {
        // return distance skim
        return autoDistance;
    }


    public static void readTripRates (ResourceBundle rb) {
        // read trip rates for local trucks

        if (ResourceUtil.getBooleanProperty(rb, "use.ln.trip.rates.for.sut.mut", false)) {
            String fileName = rb.getString("ln.trip.rates");
            lnTripRates = ncstmUtil.importTable(fileName);
        }
        String fileName = rb.getString("trip.rates");
        tripRates = ncstmUtil.importTable(fileName);
    }
    

    public static float[] getTripRates(String colName){
    	// return array of trip rates
    	return tripRates.getColumnAsFloat(colName);
    }


    public static float[] getLnTripRates(String colName){
        // return array of ln() trip rates
        return lnTripRates.getColumnAsFloat(colName);
    }


    public static String[] getTripIndustries(String colName){
    	// return array of industry types
    	return tripRates.getColumnAsString(colName);
    }


    public static String[] getLnTripIndustries(String colName){
        // return array of industry types
        return lnTripRates.getColumnAsString(colName);
    }


    public static TableDataSet readProdAndAttr(ResourceBundle rb, String fileName){
    	//reads productions and attractions
    	producePAs pas = new producePAs(rb);
    	pas.run();
    	prodAndAttr = ncstmUtil.importTable(fileName);
    	prodAndAttr.buildIndex(1);
    	return prodAndAttr;
    }
    
}
