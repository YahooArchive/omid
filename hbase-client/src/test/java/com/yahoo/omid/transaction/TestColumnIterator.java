package com.yahoo.omid.transaction;

import static org.testng.AssertJUnit.assertEquals;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.log.Log;

import com.google.common.collect.ImmutableList;

@Test(groups = "noHBase")
public class TestColumnIterator {

    final byte[] row = Bytes.toBytes("row");
    private final byte[] family1 = Bytes.toBytes("f1");
    private final byte[] family2 = Bytes.toBytes("f2");
    private final byte[] qualifier1 = Bytes.toBytes("c1");
    private final byte[] qualifier2 = Bytes.toBytes("c2");
    final byte[] data = Bytes.toBytes("data");

    private final List<Cell> cells = new ArrayList<Cell>(
            Arrays.asList(
                    // Group 1 (3 elems but grouping should filter shadow cell, so check for 2)
                    new KeyValue(row, family1, qualifier1, 0, data),
                    new KeyValue(row, family1, qualifier1, 1, data),
                    new KeyValue(row, family1, CellUtils.addShadowCellSuffix(qualifier1), 0, data),
                    // Group 2 (2 elems but grouping should filter shadow cell, so check for 1)
                    new KeyValue(row, family1, qualifier2, 0, data),
                    new KeyValue(row, family1, CellUtils.addShadowCellSuffix(qualifier2), 0, data),
                    // Group 3 (2 elems but grouping should filter shadow cell, so check for 1)
                    new KeyValue(row, family2, qualifier1, 0, data),
                    new KeyValue(row, family2, CellUtils.addShadowCellSuffix(qualifier1), 0, data)
                        )
    );

    @Test
    public void testGroupingCellsByColumnFilteringShadowCells() {

        ImmutableList<Collection<Cell>> groupedColumnsWithoutShadowCells =
                TTable.groupCellsByColumnFilteringShadowCells(cells);
        Log.info("Column Groups " + groupedColumnsWithoutShadowCells);
        assertEquals("Should be 3 column groups", 3, groupedColumnsWithoutShadowCells.size());
        int group1Counter = 0;
        int group2Counter = 0;
        int group3Counter = 0;
        for (Collection<Cell> columns : groupedColumnsWithoutShadowCells) {
            for (Cell cell : columns) {
                byte[] cellFamily = CellUtil.cloneFamily(cell);
                byte[] cellQualifier = CellUtil.cloneQualifier(cell);
                // Group 1
                if (Bytes.equals(cellFamily, family1) &&
                        Bytes.equals(cellQualifier, qualifier1)) {
                    group1Counter++;
                }
                // Group 2
                if (Bytes.equals(cellFamily, family1) &&
                        Bytes.equals(cellQualifier, qualifier2)) {
                    group2Counter++;
                }
                // Group 3
                if (Bytes.equals(cellFamily, family2) &&
                        Bytes.equals(cellQualifier, qualifier1)) {
                    group3Counter++;
                }
            }
        }

        assertEquals("Group 1 should have 2 elems", 2, group1Counter);
        assertEquals("Group 2 should have 1 elems", 1, group2Counter);
        assertEquals("Group 3 should have 1 elems", 1, group3Counter);
    }
}
