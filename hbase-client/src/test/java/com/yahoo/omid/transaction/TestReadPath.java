package com.yahoo.omid.transaction;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import org.testng.ITestContext;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;

@Test(groups = "sharedHBase")
public class TestReadPath extends OmidTestBase {

    final byte[] family = Bytes.toBytes(TEST_FAMILY);
    final byte[] row = Bytes.toBytes("row");
    private final byte[] col = Bytes.toBytes("col1");
    final byte[] data = Bytes.toBytes("data");
    private final byte[] uncommitted = Bytes.toBytes("uncommitted");

    @Test
    public void testReadInterleaved(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable table = new TTable(hbaseConf, TEST_TABLE);

        // Put some data on the DB
        Transaction t1 = tm.begin();
        Transaction t2 = tm.begin();

        Put put = new Put(row);
        put.add(family, col, data);
        table.put(t1, put);
        tm.commit(t1);

        Get get = new Get(row);
        Result result = table.get(t2, get);
        assertFalse("Should be unable to read column", result.containsColumn(family, col));
    }

    @Test
    public void testReadWithSeveralUncommitted(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable table = new TTable(hbaseConf, TEST_TABLE);
        
        // Put some data on the DB
        Transaction t = tm.begin();
        Put put = new Put(row);
        put.add(family, col, data);
        table.put(t, put);
        tm.commit(t);
        List<Transaction> running = new ArrayList<>();

        // Shade the data with uncommitted data
        for (int i = 0; i < 10; ++i) {
            t = tm.begin();
            put = new Put(row);
            put.add(family, col, uncommitted);
            table.put(t, put);
            running.add(t);
        }

        // Try to read from row, it should ignore the uncommitted data and return the original committed value
        t = tm.begin();
        Get get = new Get(row);
        Result result = table.get(t, get);
        Cell cell = result.getColumnLatestCell(family, col);
        assertNotNull("KeyValue is null", cell);
        byte[] value = CellUtil.cloneValue(cell);
        assertTrue("Read data doesn't match", Arrays.equals(data, value));
        tm.commit(t);

        table.close();

        for (Transaction r : running) {
            tm.rollback(r);
        }

    }

}
