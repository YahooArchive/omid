package com.yahoo.omid.transaction;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;

import com.yahoo.omid.transaction.HBaseUtils.CellGetter;

public class HRegionCellGetterAdapter implements CellGetter {

    private final HRegion hRegion;

    public HRegionCellGetterAdapter(HRegion hRegion) {
        this.hRegion = hRegion;
    }

    public Result get(Get get) throws IOException {
        return hRegion.get(get);
    }

}