package com.yahoo.omid.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.tm.RollbackException;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.tm.Transaction;
import com.yahoo.omid.tm.TransactionManager;

public class TestTransactionConflict extends OmidTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TestTransactionConflict.class);


    @Test
    public void runTestWriteWriteConflict() throws Exception {
        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.add(fam, col, data1);
        tt.put(t1, p);

        Put p2 = new Put(row);
        p2.add(fam, col, data2);
        tt.put(t2, p2);

        tm.commit(t2);

        try {
            tm.commit(t1);
            fail("Transaction should not commit successfully");
        } catch (RollbackException e) {
        }
    }

    @Test
    public void runTestMultiTableConflict() throws Exception {
        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);
        String table2 = TEST_TABLE + 2;

        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        if (!admin.tableExists(table2)) {
            HTableDescriptor desc = new HTableDescriptor(table2);
            HColumnDescriptor datafam = new HColumnDescriptor(TEST_FAMILY);
            datafam.setMaxVersions(Integer.MAX_VALUE);
            desc.addFamily(datafam);

            admin.createTable(desc);
        }

        if (admin.isTableDisabled(table2)) {
            admin.enableTable(table2);
        }

        TTable tt2 = new TTable(hbaseConf, table2);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] row2 = Bytes.toBytes("test-simple2");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.add(fam, col, data1);
        tt.put(t1, p);
        tt2.put(t1, p);

        Put p2 = new Put(row);
        p2.add(fam, col, data2);
        tt.put(t2, p2);
        p2 = new Put(row2);
        p2.add(fam, col, data2);
        tt2.put(t2, p2);

        tm.commit(t2);

        boolean aborted = false;
        try {
            tm.commit(t1);
            assertTrue("Transaction commited successfully", false);
        } catch (RollbackException e) {
            aborted = true;
        }
        assertTrue("Transaction didn't raise exception", aborted);

        ResultScanner rs = tt2.getHTable().getScanner(fam, col);

        int count = 0;
        Result r;
        while ((r = rs.next()) != null) {
            count += r.size();
        }
        assertEquals("Should have cell", 1, count);
    }

    @Test
    public void runTestCleanupAfterConflict() throws Exception {
        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction t1 = tm.begin();
        LOG.info("Transaction created " + t1);

        Transaction t2 = tm.begin();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.add(fam, col, data1);
        tt.put(t1, p);

        Get g = new Get(row).setMaxVersions();
        g.addColumn(fam, col);
        Result r = tt.getHTable().get(g);
        assertEquals("Unexpected size for read.", 1, r.size());
        assertTrue("Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)),
                   Bytes.equals(data1, r.getValue(fam, col)));

        Put p2 = new Put(row);
        p2.add(fam, col, data2);
        tt.put(t2, p2);

        r = tt.getHTable().get(g);
        assertEquals("Unexpected size for read.", 2, r.size());
        r = tt.get(t2, g);
        assertEquals("Unexpected size for read.", 1, r.size());
        assertTrue("Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)),
                   Bytes.equals(data2, r.getValue(fam, col)));

        tm.commit(t1);

        boolean aborted = false;
        try {
            tm.commit(t2);
            assertTrue("Transaction commited successfully", false);
        } catch (RollbackException e) {
            aborted = true;
        }
        assertTrue("Transaction didn't raise exception", aborted);

        r = tt.getHTable().get(g);
        assertEquals("Unexpected size for read.", 1, r.size());
        assertTrue("Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)),
                   Bytes.equals(data1, r.getValue(fam, col)));
    }

    @Test
    public void testCleanupWithDeleteRow() throws Exception {
        try {
            TransactionManager tm = newTransactionManager();
            TTable tt = new TTable(hbaseConf, TEST_TABLE);

            Transaction t1 = tm.begin();
            LOG.info("Transaction created " + t1);

            int rowcount = 10;
            int count = 0;

            byte[] fam = Bytes.toBytes(TEST_FAMILY);
            byte[] col = Bytes.toBytes("testdata");
            byte[] data1 = Bytes.toBytes("testWrite-1");
            byte[] data2 = Bytes.toBytes("testWrite-2");

            byte[] modrow = Bytes.toBytes("test-del" + 3);
            for (int i = 0; i < rowcount; i++) {
                byte[] row = Bytes.toBytes("test-del" + i);

                Put p = new Put(row);
                p.add(fam, col, data1);
                tt.put(t1, p);
            }
            tm.commit(t1);

            Transaction t2 = tm.begin();
            LOG.info("Transaction created " + t2);
            Delete d = new Delete(modrow);
            tt.delete(t2, d);

            ResultScanner rs = tt.getScanner(t2, new Scan());
            Result r = rs.next();
            count = 0;
            while (r != null) {
                count++;
                LOG.trace("row: " + Bytes.toString(r.getRow()) + " count: " + count);
                r = rs.next();
            }
            assertEquals("Wrong count", rowcount - 1, count);

            Transaction t3 = tm.begin();
            LOG.info("Transaction created " + t3);
            Put p = new Put(modrow);
            p.add(fam, col, data2);
            tt.put(t3, p);

            tm.commit(t3);

            boolean aborted = false;
            try {
                tm.commit(t2);
                assertTrue("Didn't abort", false);
            } catch (RollbackException e) {
                aborted = true;
            }
            assertTrue("Didn't raise exception", aborted);

            Transaction tscan = tm.begin();
            rs = tt.getScanner(tscan, new Scan());
            r = rs.next();
            count = 0;
            while (r != null) {
                count++;
                r = rs.next();
            }
            assertEquals("Wrong count", rowcount, count);

        } catch (Exception e) {
            LOG.error("Exception occurred", e);
            throw e;
        }
    }

    @Test
    public void testMultipleCellChangesOnSameRow() throws Exception {
        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Transaction t1 = tm.begin();
        Transaction t2 = tm.begin();
        LOG.info("Transactions created " + t1 + " " + t2);

        byte[] row = Bytes.toBytes("row");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col1 = Bytes.toBytes("testdata1");
        byte[] col2 = Bytes.toBytes("testdata2");
        byte[] data = Bytes.toBytes("testWrite-1");

        Put p2 = new Put(row);
        p2.add(fam, col1, data);
        tt.put(t2, p2);
        tm.commit(t2);

        Put p1 = new Put(row);
        p1.add(fam, col2, data);
        tt.put(t1, p1);
        tm.commit(t1);
    }

}
