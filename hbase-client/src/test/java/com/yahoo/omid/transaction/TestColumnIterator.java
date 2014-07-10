package com.yahoo.omid.transaction;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.yahoo.omid.transaction.TTable.IterableColumn;

public class TestColumnIterator {

    final byte[] row = Bytes.toBytes("row");
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier1 = Bytes.toBytes("c1");
    final byte[] qualifier2 = Bytes.toBytes("c2");
    final byte[] data = Bytes.toBytes("data");
    
    final List<Cell> cells = new ArrayList<Cell>(
            Arrays.asList(
                    new KeyValue(row, family1, qualifier1, 0, data),
                    new KeyValue(row, family1, qualifier1, 1, data),
                    new KeyValue(row, family1, qualifier2, 0, data),
                    new KeyValue(row, family2, qualifier1, 0, data)
                        )
    );
    
    @Test
    public void testBasicFunctionality() {

        IterableColumn columns = new TTable.IterableColumn(cells);
        Iterator<List<Cell>> iterator = columns.iterator();
        int columnCount = 0;
        while (iterator.hasNext()) {
            columnCount++;
            List<Cell> columnCells = iterator.next();
            switch (columnCount) {
            case 1:
                assertEquals("Should be 2", 2, columnCells.size());
                break;
            case 2:
                assertEquals("Should be 1", 1, columnCells.size());
                break;
            case 3:
                assertEquals("Should be 1", 1, columnCells.size());
                break;
            default:
                fail();
            }

        }
        assertEquals("Should be 3", 3, columnCount);
    }

}
