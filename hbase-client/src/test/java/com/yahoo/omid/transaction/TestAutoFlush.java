package com.yahoo.omid.transaction;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "sharedHBase")
public class TestAutoFlush extends OmidTestBase {

    @Test
    public void testReadWithSeveralUncommitted(ITestContext context) throws Exception {
        byte[] family = Bytes.toBytes(TEST_FAMILY);
        byte[] row = Bytes.toBytes("row");
        byte[] col = Bytes.toBytes("col1");
        byte[] data = Bytes.toBytes("data");
        TransactionManager tm = newTransactionManager(context);
        TTable table = new TTable(hbaseConf, TEST_TABLE);

        // Turn off autoflush
        table.setAutoFlush(false);

        Transaction t = tm.begin();
        Put put = new Put(row);
        put.add(family, col, data);
        table.put(t, put);

        // Data shouldn't be in DB yet
        Get get = new Get(row);
        Result result = table.getHTable().get(get);
        assertEquals("Writes are already in DB", 0, result.size());

        tm.commit(t);

        // After commit, both the cell and shadow cell should be there.
        // That's why we check for two elements in the test assertion
        result = table.getHTable().get(get);
        assertEquals("Writes were not flushed to DB", 2, result.size());
    }

}
