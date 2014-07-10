package com.yahoo.omid.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import junit.framework.Assert;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;

public class TestBasicTransaction extends OmidTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TestBasicTransaction.class);


    @Test
    public void testTimestampsOfTwoRowsInstertedAfterCommitOfSingleTransactionAreEquals()
            throws Exception {

        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] rowName2 = Bytes.toBytes("row2");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        Transaction tx1 = tm.begin();

        Put row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue1);
        tt.put(tx1, row1);
        Put row2 = new Put(rowName2);
        row2.add(famName1, colName1, dataValue2);
        tt.put(tx1, row2);

        tm.commit(tx1);

        tt.close();

        // Checks
        Get getResultRow1 = new Get(rowName1).setMaxVersions(1);
        Result result1 = tt.getHTable().get(getResultRow1);
        byte[] val1 = result1.getValue(famName1, colName1);
        assertTrue(
                   "Unexpected value for row 1 in col 1: " + Bytes.toString(val1),
                   Bytes.equals(dataValue1, result1.getValue(famName1, colName1)));
        long tsRow1 = result1.rawCells()[0].getTimestamp();

        Get getResultRow2 = new Get(rowName2).setMaxVersions(1);
        Result result2 = tt.getHTable().get(getResultRow2);
        byte[] val2 = result2.getValue(famName1, colName1);
        assertTrue(
                   "Unexpected value for row 2 in col 1: " + Bytes.toString(val2),
                   Bytes.equals(dataValue2, result2.getValue(famName1, colName1)));
        long tsRow2 = result2.rawCells()[0].getTimestamp();

        assertEquals("Timestamps of row 1 and row 2 are different", tsRow1,
                     tsRow2);

    }

    @Test
    public void testTimestampsOfTwoRowsModifiedByTwoSequentialTransactionsAreEqualAndHaveBeenIncreasedMonotonically()
            throws Exception {

        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        byte[] rowName1 = Bytes.toBytes("row1");
        byte[] rowName2 = Bytes.toBytes("row2");
        byte[] famName1 = Bytes.toBytes(TEST_FAMILY);
        byte[] colName1 = Bytes.toBytes("col1");
        byte[] dataValue1 = Bytes.toBytes("testWrite-1");
        byte[] dataValue2 = Bytes.toBytes("testWrite-2");

        byte[] dataValue3 = Bytes.toBytes("testWrite-3");
        byte[] dataValue4 = Bytes.toBytes("testWrite-4");

        Transaction tx1 = tm.begin();

        Put row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue1);
        tt.put(tx1, row1);
        Put row2 = new Put(rowName2);
        row2.add(famName1, colName1, dataValue2);
        tt.put(tx1, row2);

        tm.commit(tx1);

        Transaction tx2 = tm.begin();

        row1 = new Put(rowName1);
        row1.add(famName1, colName1, dataValue3);
        tt.put(tx2, row1);
        row2 = new Put(rowName2);
        row2.add(famName1, colName1, dataValue4);
        tt.put(tx2, row2);

        tm.commit(tx2);

        tt.close();

        // Checks
        Get getResultRow1 = new Get(rowName1).setMaxVersions(2);
        Result result1 = tt.getHTable().get(getResultRow1);
        byte[] val1 = result1.getValue(famName1, colName1);
        assertTrue(
                   "Unexpected value for row 1 in col 1: " + Bytes.toString(val1),
                   Bytes.equals(dataValue3, result1.getValue(famName1, colName1)));

        long lastTsRow1 = result1.rawCells()[0].getTimestamp();
        long previousTsRow1 = result1.rawCells()[1].getTimestamp();

        Get getResultRow2 = new Get(rowName2).setMaxVersions(2);
        Result result2 = tt.getHTable().get(getResultRow2);
        byte[] val2 = result2.getValue(famName1, colName1);
        assertTrue(
                   "Unexpected value for row 2 in col 1: " + Bytes.toString(val2),
                   Bytes.equals(dataValue4, result2.getValue(famName1, colName1)));

        long lastTsRow2 = result2.rawCells()[0].getTimestamp();
        long previousTsRow2 = result2.rawCells()[1].getTimestamp();

        assertTrue(
                   "Timestamps assigned by Tx2 to row 1 and row 2 are different",
                   lastTsRow1 == lastTsRow2);
        assertTrue(
                   "Timestamps assigned by Tx2 to row 1 and row 2 are different",
                   previousTsRow1 == previousTsRow2);
        assertTrue(
                   "Timestamp assigned by Tx2 to row 1 has not been increased monotonically",
                   lastTsRow1 > previousTsRow1);
        assertTrue(
                   "Timestamp assigned by Tx2 to row 2 has not been increased monotonically",
                   lastTsRow2 > previousTsRow2);

    }

    @Test
    public void runTestSimple() throws Exception {
        try {
            TransactionManager tm = newTransactionManager();
            TTable tt = new TTable(hbaseConf, TEST_TABLE);
         
            Transaction t1 = tm.begin();
            LOG.info("Transaction created " + t1);
         
            byte[] row = Bytes.toBytes("test-simple");
            byte[] fam = Bytes.toBytes(TEST_FAMILY);
            byte[] col = Bytes.toBytes("testdata");
            byte[] data1 = Bytes.toBytes("testWrite-1");
            byte[] data2 = Bytes.toBytes("testWrite-2");

            Put p = new Put(row);
            p.add(fam, col, data1);
            tt.put(t1, p);
            tm.commit(t1);

            Transaction tread = tm.begin();
            Transaction t2 = tm.begin();
            p = new Put(row);
            p.add(fam, col, data2);
            tt.put(t2, p);
            tm.commit(t2);

            Get g = new Get(row).setMaxVersions(1);
            Result r = tt.getHTable().get(g);
            assertTrue("Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)),
                       Bytes.equals(data2, r.getValue(fam, col)));

            r = tt.get(tread, g);
            assertTrue("Unexpected value for SI read " + tread + ": " + Bytes.toString(r.getValue(fam, col)),
                       Bytes.equals(data1, r.getValue(fam, col)));
        } catch (Exception e) {
            LOG.error("Exception occurred", e);
            throw e;
        }
    }

    @Test
    public void runTestManyVersions() throws Exception {
        try {
            TransactionManager tm = newTransactionManager();
            TTable tt = new TTable(hbaseConf, TEST_TABLE);

            Transaction t1 = tm.begin();
            LOG.info("Transaction created " + t1);

            byte[] row = Bytes.toBytes("test-simple");
            byte[] fam = Bytes.toBytes(TEST_FAMILY);
            byte[] col = Bytes.toBytes("testdata");
            byte[] data1 = Bytes.toBytes("testWrite-1");
            byte[] data2 = Bytes.toBytes("testWrite-2");

            Put p = new Put(row);
            p.add(fam, col, data1);
            tt.put(t1, p);
            tm.commit(t1);

            for (int i = 0; i < 5; ++i) {
                Transaction t2 = tm.begin();
                p = new Put(row);
                p.add(fam, col, data2);
                tt.put(t2, p);
            }
            Transaction tread = tm.begin();

            Get g = new Get(row).setMaxVersions(1);
            Result r = tt.getHTable().get(g);
            assertTrue("Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)),
                       Bytes.equals(data2, r.getValue(fam, col)));

            r = tt.get(tread, g);
            assertTrue("Unexpected value for SI read " + tread + ": " + Bytes.toString(r.getValue(fam, col)),
                       Bytes.equals(data1, r.getValue(fam, col)));
        } catch (Exception e) {
            LOG.error("Exception occurred", e);
            throw e;
        }
    }

    @Test
    public void runTestInterleave() throws Exception {
        try {
            TransactionManager tm = newTransactionManager();
            TTable tt = new TTable(hbaseConf, TEST_TABLE);
        
            Transaction t1 = tm.begin();
            LOG.info("Transaction created " + t1);
         
            byte[] row = Bytes.toBytes("test-interleave");
            byte[] fam = Bytes.toBytes(TEST_FAMILY);
            byte[] col = Bytes.toBytes("testdata");
            byte[] data1 = Bytes.toBytes("testWrite-1");
            byte[] data2 = Bytes.toBytes("testWrite-2");

            Put p = new Put(row);
            p.add(fam, col, data1);
            tt.put(t1, p);
            tm.commit(t1);

            Transaction t2 = tm.begin();
            p = new Put(row);
            p.add(fam, col, data2);
            tt.put(t2, p);

            Transaction tread = tm.begin();
            Get g = new Get(row).setMaxVersions(1);
            Result r = tt.get(tread, g);
            assertTrue("Unexpected value for SI read " + tread + ": " + Bytes.toString(r.getValue(fam, col)),
                       Bytes.equals(data1, r.getValue(fam, col)));
            tm.commit(t2);

            r = tt.getHTable().get(g);
            assertTrue("Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)),
                       Bytes.equals(data2, r.getValue(fam, col)));

        } catch (Exception e) {
            LOG.error("Exception occurred", e);
            throw e;
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSameCommitRaisesException() throws Exception  {
        TransactionManager tm = newTransactionManager();

        Transaction t1 = tm.begin();
        tm.commit(t1);
        tm.commit(t1);
    }

    @Test
    public void runTestInterleaveScan() throws Exception {
        try {
            TransactionManager tm = newTransactionManager();
            TTable tt = new TTable(hbaseConf, TEST_TABLE);
         
            Transaction t1 = tm.begin();
            LOG.info("Transaction created " + t1);
         
            byte[] fam = Bytes.toBytes(TEST_FAMILY);
            byte[] col = Bytes.toBytes("testdata");
            byte[] data1 = Bytes.toBytes("testWrite-1");
            byte[] data2 = Bytes.toBytes("testWrite-2");
         
            byte[] startrow = Bytes.toBytes("test-scan" + 0);
            byte[] stoprow = Bytes.toBytes("test-scan" + 9);
            byte[] modrow = Bytes.toBytes("test-scan" + 3);
            for (int i = 0; i < 10; i++) {
                byte[] row = Bytes.toBytes("test-scan" + i);
            
                Put p = new Put(row);
                p.add(fam, col, data1);
                tt.put(t1, p);
            }
            tm.commit(t1);

            Transaction t2 = tm.begin();
            Put p = new Put(modrow);
            p.add(fam, col, data2);
            tt.put(t2, p);
         
            Transaction tscan = tm.begin();
            ResultScanner rs = tt.getScanner(tscan, new Scan().setStartRow(startrow).setStopRow(stoprow));
            Result r = rs.next();
            int i = 0;
            while (r != null) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Scan1 :" + Bytes.toString(r.getRow()) + " => " + Bytes.toString(r.getValue(fam, col)));
                }
                LOG.debug(""+ ++i);

                assertTrue("Unexpected value for SI scan " + tscan + ": " + Bytes.toString(r.getValue(fam, col)),
                           Bytes.equals(data1, r.getValue(fam, col)));
                r = rs.next();
            }
            tm.commit(t2);

            int modifiedrows = 0;
            tscan = tm.begin();
            rs = tt.getScanner(tscan, new Scan().setStartRow(startrow).setStopRow(stoprow));
            r = rs.next();
            while (r != null) {
                if (Bytes.equals(data2, r.getValue(fam, col))) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Modified :" + Bytes.toString(r.getRow()));
                    }
                    modifiedrows++;
                }
            
                r = rs.next();
            }
         
            assertTrue("Expected 1 row modified, but " + modifiedrows + " are.", 
                       modifiedrows == 1);

        } catch (Exception e) {
            LOG.error("Exception occurred", e);
            throw e;
        }
    }
   
    @Test
    public void testUserOperationsDontAllowTimestampSpecification() throws Exception {

        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        TransactionManager tm = newTransactionManager();

        long randomTimestampValue = Bytes.toLong("deadbeef".getBytes());

        byte[] row = Bytes.toBytes("row1");
        byte[] famName = Bytes.toBytes(TEST_FAMILY);
        byte[] colName = Bytes.toBytes("col1");
        byte[] dataValue = Bytes.toBytes("testWrite-1");

        Transaction tx = tm.begin();

        // Test put fails when a timestamp is specified in the put
        Put put = new Put(row, randomTimestampValue);
        put.add(famName, colName, dataValue);
        try {
            tt.put(tx, put);
            Assert.fail("Should have thrown an IllegalArgumentException due to timestamp specification");
        } catch (IllegalArgumentException e) {
            // Continue
        }

        // Test put fails when a timestamp is specified in a qualifier
        put = new Put(row);
        put.add(famName, colName, randomTimestampValue, dataValue);
        try {
            tt.put(tx, put);
            Assert.fail("Should have thrown an IllegalArgumentException due to timestamp specification");
        } catch (IllegalArgumentException e) {
            // Continue
        }

        // Test that get fails when a timestamp is specified
        Get get = new Get(row);
        get.setTimeStamp(randomTimestampValue);
        try {
            tt.get(tx, get);
            Assert.fail("Should have thrown an IllegalArgumentException due to timestamp specification");
        } catch (IllegalArgumentException e) {
            // Continue
        }

        // Test scan fails when a timerange is specified
        Scan scan = new Scan(get);
        try {
            tt.getScanner(tx, scan);
            Assert.fail("Should have thrown an IllegalArgumentException due to timestamp specification");
        } catch (IllegalArgumentException e) {
            // Continue
        }

        // Test delete fails when a timestamp is specified
        Delete delete = new Delete(row);
        delete.setTimestamp(randomTimestampValue);
        try {
            tt.delete(tx, delete);
            Assert.fail("Should have thrown an IllegalArgumentException due to timestamp specification");
        } catch (IllegalArgumentException e) {
            // Continue
        }

        // Test delete fails when a timestamp is specified in a qualifier
        delete = new Delete(row);
        delete.deleteColumn(famName, colName, randomTimestampValue);
        try {
            tt.delete(tx, delete);
            Assert.fail("Should have thrown an IllegalArgumentException due to timestamp specification");
        } catch (IllegalArgumentException e) {
            // Continue
        }

    }

}
