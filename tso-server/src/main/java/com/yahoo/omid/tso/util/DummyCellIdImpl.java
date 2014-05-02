package com.yahoo.omid.tso.util;

import com.yahoo.omid.tso.CellId;

public class DummyCellIdImpl implements CellId {

    private final long cellId;

    public DummyCellIdImpl(long cellId) {
        this.cellId = cellId;
    }

    @Override
    public long getCellId() {
        return cellId;
    }

}