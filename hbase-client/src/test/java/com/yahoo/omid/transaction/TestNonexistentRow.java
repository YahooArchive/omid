package com.yahoo.omid.transaction;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "sharedHBase")
public class TestNonexistentRow extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestNonexistentRow.class);


    @Test
    public void testMultiPutSameRow(ITestContext context) throws Exception {
        try {
            TransactionManager tm = newTransactionManager(context);
            TTable table1 = new TTable(hbaseConf, TEST_TABLE);

            int num = 10;
            Transaction t = tm.begin();
            for (int j = 0; j < num; j++) {
                byte[] data = Bytes.toBytes(j);
                Put put = new Put(data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value"), data);
                table1.put(t, put);
            }
            int key = 15;
            Get g = new Get(Bytes.toBytes(key));
            Result r = table1.get(t, g);

            assertTrue("Found a row that should not exist", r.isEmpty());

            tm.commit(t);
        } catch (Exception e) {
            LOG.error("Exception in test", e);
            throw e;
        }
    }

}
