package com.yahoo.omid.transaction;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.mockito.Mockito;

import com.yahoo.omid.client.TSOClient;
import com.yahoo.omid.transaction.TTable.IterableColumn;

public class TestColumnIterator {

    final byte[] row = Bytes.toBytes("row");
    final byte[] family1 = Bytes.toBytes("f1");
    final byte[] family2 = Bytes.toBytes("f2");
    final byte[] qualifier1 = Bytes.toBytes("c1");
    final byte[] qualifier2 = Bytes.toBytes("c2");
    final byte[] data = Bytes.toBytes("data");
    
    final List<KeyValue> keyValues = new ArrayList<KeyValue>(
            Arrays.asList(
                    new KeyValue(row, family1, qualifier1, 0, data),
                    new KeyValue(row, family1, qualifier1, 1, data),
                    new KeyValue(row, family1, qualifier2, 0, data),
                    new KeyValue(row, family2, qualifier1, 0, data)
                        )
    );
    
    @Test
    public void testBasicFunctionality() {

        IterableColumn columns = new TTable.IterableColumn(keyValues);
        Iterator<List<KeyValue>> iterator = columns.iterator();
        int columnCount = 0;
        while (iterator.hasNext()) {
            columnCount++;
            List<KeyValue> columnKeyValues = iterator.next();
            switch (columnCount) {
            case 1:
                assertEquals("Should be 2", 2, columnKeyValues.size());
                break;
            case 2:
                assertEquals("Should be 1", 1, columnKeyValues.size());
                break;
            case 3:
                assertEquals("Should be 1", 1, columnKeyValues.size());
                break;
            default:
                fail();
            }

        }
        assertEquals("Should be 3", 3, columnCount);
    }

    @Test
    public void testNewFilter() throws Exception {
        
        TSOClient tsoClient = Mockito.mock(TSOClient.class);
        
        Transaction tx1 = new Transaction(0, tsoClient);
        
        // List<KeyValue> TTable.filterRightQualifierSnapshotValue

    }
}
