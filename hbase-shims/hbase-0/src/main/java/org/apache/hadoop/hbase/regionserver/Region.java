package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

public class Region {

    HRegion hRegion;

    public Region(HRegion hRegion) {

        this.hRegion = hRegion;

    }

    Result get(Get getOperation) throws IOException {

        return hRegion.get(getOperation);

    }

    HRegionInfo getRegionInfo() {

        return hRegion.getRegionInfo();

    }
}