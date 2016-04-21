/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.transaction;

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

import static org.junit.Assert.fail;
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
    public void testInterleavedScanReturnsTheRightSnapshotResults(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        // Basic data-scaffolding for test
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("TEST_COL");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        byte[] startRow = Bytes.toBytes("row-to-scan" + 0);
        byte[] stopRow = Bytes.toBytes("row-to-scan" + 9);
        byte[] randomRow = Bytes.toBytes("row-to-scan" + 3);

        // Add some data transactionally to have an initial state for the test
        Transaction tx1 = tm.begin();
        for (int i = 0; i < 10; i++) {
            byte[] row = Bytes.toBytes("row-to-scan" + i);

            Put p = new Put(row);
            p.add(fam, col, data1);
            txTable.put(tx1, p);
        }
        tm.commit(tx1);

        // Start a second transaction -Tx2- modifying a random row and check that a concurrent transactional context
        // that scans the table, gets the proper snapshot with the stuff written by Tx1
        Transaction tx2 = tm.begin();
        Put p = new Put(randomRow);
        p.add(fam, col, data2);
        txTable.put(tx2, p);

        Transaction scanTx = tm.begin(); // This is the concurrent transactional scanner
        ResultScanner rs = txTable.getScanner(scanTx, new Scan().setStartRow(startRow).setStopRow(stopRow));
        Result r = rs.next(); // Exercise the next() method
        int i = 0;
        while (r != null) {
            LOG.trace("Scan (" + ++i + ")" + Bytes.toString(r.getRow()) + " => " + Bytes.toString(r.getValue(fam, col)));
            assertTrue(Bytes.equals(data1, r.getValue(fam, col)),
                    "Unexpected value for SI scan " + scanTx + ": " + Bytes.toString(r.getValue(fam, col)));
            r = rs.next();
        }

        // Commit the Tx2 and then check that under a new transactional context, the scanner gets the right snapshot,
        // which must include the row modified by Tx2
        tm.commit(tx2);

        int modifiedRows = 0;
        Transaction newScanTx = tm.begin();
        ResultScanner newRS = txTable.getScanner(newScanTx, new Scan().setStartRow(startRow).setStopRow(stopRow));
        Result[] results = newRS.next(10); // Exercise the next(numRows) method
        for (Result result : results) {
            if (Bytes.equals(data2, result.getValue(fam, col))) {
                LOG.trace("Modified :" + Bytes.toString(result.getRow()));
                modifiedRows++;
            }
        }
        assertEquals(modifiedRows, 1, "Expected 1 row modified, but " + modifiedRows + " are.");

        // Same check as before but checking that the results are correct when retrieved through the Scanner Iterator
        modifiedRows = 0;
        ResultScanner iterableRS = txTable.getScanner(newScanTx, new Scan().setStartRow(startRow).setStopRow(stopRow));
        for (Result res : iterableRS) {
            if (Bytes.equals(data2, res.getValue(fam, col))) {
                LOG.trace("Modified :" + Bytes.toString(res.getRow()));
                modifiedRows++;
            }
        }

        assertEquals(modifiedRows, 1, "Expected 1 row modified, but " + modifiedRows + " are.");

        // Finally, check that the Scanner Iterator does not implement the remove method
        try {
            iterableRS.iterator().remove();
            fail();
        } catch (RuntimeException re) {
            // Expected
        }

    }

    @Test(timeOut = 30_000)
    public void testInterleavedScanReturnsTheRightSnapshotResultsWhenATransactionAborts(ITestContext context)
            throws Exception {

        TransactionManager tm = newTransactionManager(context);
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        // Basic data-scaffolding for test
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("TEST_COL");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        byte[] startRow = Bytes.toBytes("row-to-scan" + 0);
        byte[] stopRow = Bytes.toBytes("row-to-scan" + 9);
        byte[] randomRow = Bytes.toBytes("row-to-scan" + 3);

        // Add some data transactionally to have an initial state for the test
        Transaction tx1 = tm.begin();
        for (int i = 0; i < 10; i++) {
            byte[] row = Bytes.toBytes("row-to-scan" + i);

            Put p = new Put(row);
            p.add(fam, col, data1);
            txTable.put(tx1, p);
        }
        tm.commit(tx1);

        // Start a second transaction modifying a random row and check that a transactional scanner in Tx2 gets the
        // right snapshot with the new value in the random row just written by Tx2
        Transaction tx2 = tm.begin();
        Put p = new Put(randomRow);
        p.add(fam, col, data2);
        txTable.put(tx2, p);

        int modifiedRows = 0;
        ResultScanner rs = txTable.getScanner(tx2, new Scan().setStartRow(startRow).setStopRow(stopRow));
        Result r = rs.next();
        while (r != null) {
            if (Bytes.equals(data2, r.getValue(fam, col))) {
                LOG.trace("Modified :" + Bytes.toString(r.getRow()));
                modifiedRows++;
            }

            r = rs.next();
        }

        assertEquals(modifiedRows, 1, "Expected 1 row modified, but " + modifiedRows + " are.");

        // Rollback the second transaction and then check that under a new transactional scanner we get the snapshot
        // that includes the only the initial rows put by Tx1
        tm.rollback(tx2);

        Transaction txScan = tm.begin();
        rs = txTable.getScanner(txScan, new Scan().setStartRow(startRow).setStopRow(stopRow));
        r = rs.next();
        while (r != null) {
            LOG.trace("Scan1 :" + Bytes.toString(r.getRow()) + " => " + Bytes.toString(r.getValue(fam, col)));
            assertTrue(Bytes.equals(data1, r.getValue(fam, col)),
                    "Unexpected value for SI scan " + txScan + ": " + Bytes.toString(r.getValue(fam, col)));
            r = rs.next();
        }

        // Same check as before but checking that the results are correct when retrieved through the Scanner Iterator
        ResultScanner iterableRS = txTable.getScanner(txScan, new Scan().setStartRow(startRow).setStopRow(stopRow));
        for (Result result : iterableRS) {
            assertTrue(Bytes.equals(data1, result.getValue(fam, col)),
                    "Unexpected value for SI scan " + txScan + ": " + Bytes.toString(result.getValue(fam, col)));
        }

        // Finally, check that the Scanner Iterator does not implement the remove method
        try {
            iterableRS.iterator().remove();
            fail();
        } catch (RuntimeException re) {
            // Expected
        }

    }

}
