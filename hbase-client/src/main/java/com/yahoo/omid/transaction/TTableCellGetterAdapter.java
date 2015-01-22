package com.yahoo.omid.transaction;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import com.yahoo.omid.transaction.CellUtils.CellGetter;

public class TTableCellGetterAdapter implements CellGetter {

    private final TTable txTable;

    public TTableCellGetterAdapter(TTable txTable) {
        this.txTable = txTable;
    }

    public Result get(Get get) throws IOException {
        return txTable.getHTable().get(get);
    }

}