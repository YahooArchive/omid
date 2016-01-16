package com.yahoo.omid.transaction;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "sharedHBase")
public class TestBasicTransaction extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestBasicTransaction.class);


    @Test(timeOut = 30_000)
    public void testTimestampsOfTwoRowsInstertedAfterCommitOfSingleTransactionAreEquals(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
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
        assertTrue(Bytes.equals(dataValue1, result1.getValue(famName1, colName1)),
                   "Unexpected value for row 1 in col 1: " + Bytes.toString(val1));
        long tsRow1 = result1.rawCells()[0].getTimestamp();

        Get getResultRow2 = new Get(rowName2).setMaxVersions(1);
        Result result2 = tt.getHTable().get(getResultRow2);
        byte[] val2 = result2.getValue(famName1, colName1);
        assertTrue(Bytes.equals(dataValue2, result2.getValue(famName1, colName1)),
                   "Unexpected value for row 2 in col 1: " + Bytes.toString(val2));
        long tsRow2 = result2.rawCells()[0].getTimestamp();

        assertEquals(tsRow2, tsRow1, "Timestamps of row 1 and row 2 are different");

    }

    @Test(timeOut = 30_000)
    public void testTimestampsOfTwoRowsModifiedByTwoSequentialTransactionsAreEqualAndHaveBeenIncreasedMonotonically(ITestContext context)
        throws Exception {

        TransactionManager tm = newTransactionManager(context);
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
        assertTrue(Bytes.equals(dataValue3, result1.getValue(famName1, colName1)),
                   "Unexpected value for row 1 in col 1: " + Bytes.toString(val1));

        long lastTsRow1 = result1.rawCells()[0].getTimestamp();
        long previousTsRow1 = result1.rawCells()[1].getTimestamp();

        Get getResultRow2 = new Get(rowName2).setMaxVersions(2);
        Result result2 = tt.getHTable().get(getResultRow2);
        byte[] val2 = result2.getValue(famName1, colName1);
        assertTrue(Bytes.equals(dataValue4, result2.getValue(famName1, colName1)),
                   "Unexpected value for row 2 in col 1: " + Bytes.toString(val2));

        long lastTsRow2 = result2.rawCells()[0].getTimestamp();
        long previousTsRow2 = result2.rawCells()[1].getTimestamp();

        assertTrue(lastTsRow1 == lastTsRow2, "Timestamps assigned by Tx2 to row 1 and row 2 are different");
        assertTrue(previousTsRow1 == previousTsRow2, "Timestamps assigned by Tx2 to row 1 and row 2 are different");
        assertTrue(lastTsRow1 > previousTsRow1, "Timestamp assigned by Tx2 to row 1 hasn't increased monotonically");
        assertTrue(lastTsRow2 > previousTsRow2, "Timestamp assigned by Tx2 to row 2 hasn't increased monotonically");

    }

    @Test(timeOut = 30_000)
    public void runTestSimple(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);

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
        assertTrue(Bytes.equals(data2, r.getValue(fam, col)),
                   "Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)));

        r = tt.get(tread, g);
        assertTrue(Bytes.equals(data1, r.getValue(fam, col)),
                   "Unexpected value for SI read " + tread + ": " + Bytes.toString(r.getValue(fam, col)));
    }

    @Test(timeOut = 30_000)
    public void runTestManyVersions(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
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
        assertTrue(Bytes.equals(data2, r.getValue(fam, col)),
                   "Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)));

        r = tt.get(tread, g);
        assertTrue(Bytes.equals(data1, r.getValue(fam, col)),
                   "Unexpected value for SI read " + tread + ": " + Bytes.toString(r.getValue(fam, col)));

    }

    @Test(timeOut = 30_000)
    public void runTestInterleave(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
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
        assertTrue(Bytes.equals(data1, r.getValue(fam, col)),
                   "Unexpected value for SI read " + tread + ": " + Bytes.toString(r.getValue(fam, col)));
        tm.commit(t2);

        r = tt.getHTable().get(g);
        assertTrue(Bytes.equals(data2, r.getValue(fam, col)),
                   "Unexpected value for read: " + Bytes.toString(r.getValue(fam, col)));

    }

    @Test(expectedExceptions = IllegalArgumentException.class, timeOut = 30_000)
    public void testSameCommitRaisesException(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);

        Transaction t1 = tm.begin();
        tm.commit(t1);
        tm.commit(t1);
    }

    @Test(timeOut = 30_000)
    public void runTestInterleaveScan(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
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
            LOG.debug("" + ++i);

            assertTrue(Bytes.equals(data1, r.getValue(fam, col)),
                       "Unexpected value for SI scan " + tscan + ": " + Bytes.toString(r.getValue(fam, col)));
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

        assertTrue(modifiedrows == 1, "Expected 1 row modified, but " + modifiedrows + " are.");

    }

    @Test(timeOut = 30_000)
    public void runTestInterleaveScanWhenATransactionAborts(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
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

        int modifiedrows = 0;
        ResultScanner rs = tt.getScanner(t2, new Scan().setStartRow(startrow).setStopRow(stoprow).addColumn(fam, col));
        Result r = rs.next();
        while (r != null) {
            if (Bytes.equals(data2, r.getValue(fam, col))) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Modified :" + Bytes.toString(r.getRow()));
                }
                modifiedrows++;
            }

            r = rs.next();
        }

        assertTrue(modifiedrows == 1, "Expected 1 row modified, but " + modifiedrows + " are.");
        tm.rollback(t2);

        Transaction tscan = tm.begin();
        rs = tt.getScanner(tscan, new Scan().setStartRow(startrow).setStopRow(stoprow).addColumn(fam, col));
        r = rs.next();
        while (r != null) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Scan1 :" + Bytes.toString(r.getRow()) + " => " + Bytes.toString(r.getValue(fam, col)));
            }

            assertTrue(Bytes.equals(data1, r.getValue(fam, col)),
                       "Unexpected value for SI scan " + tscan + ": " + Bytes.toString(r.getValue(fam, col)));
            r = rs.next();
        }

    }


}
