package com.pb.projects.ncstm.national;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to store data of distribution centers for truck flows for the North Carolina Statewide Model (NCSTM)
 * Author: Rolf Moeckel, PB Albuquerque
 * Data: 26 January 2012
 */

public class distributionCenters {

    private static final Map<Integer, distributionCenters> dcMap = new HashMap<>();
    private int id;
    private int taz;
    private float size;
    private int fafZone;


    public distributionCenters (int id, int taz, float size, int fafZone) {
        // Create new distribution center
        this.id = id;
        this.taz = taz;
        this.size = size;
        this.fafZone = fafZone;
        dcMap.put(id, this);
    }

    public int getId () {
        return id;
    }

    public int getTaz () {
        return taz;
    }

    public float getSize () {
        return size;
    }

    public int getFafZone () {
        return fafZone;
    }

    public static distributionCenters[] getDistributionCenterArray() {
        return dcMap.values().toArray(new distributionCenters[dcMap.size()]);
    }

}
