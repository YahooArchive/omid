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

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.testng.ITestContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.slf4j.LoggerFactory.getLogger;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

/**
 * These tests try to analyze the transactional anomalies described by P. Baillis et al. in
 * http://arxiv.org/pdf/1302.0309.pdf
 *
 * These tests try to model what project Hermitage is trying to do to compare the behavior of different DBMSs on these
 * anomalies depending on the different isolation levels they offer. For more info on the Hermitage project, please
 * refer to: https://github.com/ept/hermitage
 *
 * Transactional histories have been translated to HBase from the ones done for Postgresql in the Hermitage project:
 * https://github.com/ept/hermitage/blob/master/postgres.md
 *
 * The "repeatable read" Postgresql isolation level is equivalent to "snapshot isolation", so we include the experiments
 * for that isolation level
 *
 * With HBase 0.98 interfaces is not possible to execute updates/deletes based on predicates so the examples here are
 * not exactly the same as in Postgres
 */
@Test(groups = "sharedHBase")
public class TestBaillisAnomaliesWithTXs extends OmidTestBase {

    private static final Logger LOG = getLogger(TestBaillisAnomaliesWithTXs.class);
    private static final String TEST_COLUMN = "baillis-col";


    // Data used in the tests
    private byte[] famName = Bytes.toBytes(TEST_FAMILY);
    private byte[] colName = Bytes.toBytes(TEST_COLUMN);

    private byte[] rowId1 = Bytes.toBytes("row1");
    private byte[] rowId2 = Bytes.toBytes("row2");
    private byte[] rowId3 = Bytes.toBytes("row3");

    private byte[] dataValue1 = Bytes.toBytes(10);
    private byte[] dataValue2 = Bytes.toBytes(20);
    private byte[] dataValue3 = Bytes.toBytes(30);


    @Test
    public void testSIPreventsPredicateManyPrecedersForReadPredicates(ITestContext context) throws Exception {
        // TX History for PMP for Read Predicate:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where value = 30; -- T1. Returns nothing
        // insert into test (id, value) values(3, 30); -- T2
        // commit; -- T2
        // select * from test where value % 3 = 0; -- T1. Still returns nothing
        // commit; -- T1

        // 0) Start transactions
        TransactionManager tm = newTransactionManager(context);
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        // 1) select * from test where value = 30; -- T1. Returns nothing
        Scan scan = new Scan();
        Filter f = new SingleColumnValueFilter(famName, colName, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(30));
        scan.setFilter(f);
        ResultScanner tx1Scanner = txTable.getScanner(tx1, scan);
        assertNull(tx1Scanner.next());

        // 2) insert into test (id, value) values(3, 30); -- T2
        Put newRow = new Put(rowId3);
        newRow.add(famName, colName, dataValue3);
        txTable.put(tx2, newRow);

        // 3) Commit TX 2
        tm.commit(tx2);

        // 4) select * from test where value % 3 = 0; -- T1. Still returns nothing
        tx1Scanner = txTable.getScanner(tx1, scan);
        assertNull(tx1Scanner.next());

        // 5) Commit TX 1
        tm.commit(tx1);
    }

    @Test
    public void testSIPreventsPredicateManyPrecedersForWritePredicates(ITestContext context) throws Exception {
        // TX History for PMP for Write Predicate:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // update test set value = value + 10; -- T1
        // delete from test where value = 20; -- T2, BLOCKS
        // commit; -- T1. T2 now prints out "ERROR: could not serialize access due to concurrent update"
        // abort; -- T2. There's nothing else we can do, this transaction has failed

        // 0) Start transactions
        TransactionManager tm = newTransactionManager(context);
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        // 1) update test set value = value + 10; -- T1
        Scan updateScan = new Scan();
        ResultScanner tx1Scanner = txTable.getScanner(tx2, updateScan);
        Result updateRes = tx1Scanner.next();
        int count = 0;
        while (updateRes != null) {
            LOG.info("RESSS {}", updateRes);
            Put row = new Put(updateRes.getRow());
            int val = Bytes.toInt(updateRes.getValue(famName, colName));
            LOG.info("Updating row id {} with value {}", Bytes.toString(updateRes.getRow()), val);
            row.add(famName, colName, Bytes.toBytes(val + 10));
            txTable.put(tx1, row);
            updateRes = tx1Scanner.next();
            count++;
        }
        assertEquals(count, 2);

        // 2) delete from test where value = 20; -- T2, BLOCKS
        Scan scan = new Scan();
        Filter f = new SingleColumnValueFilter(famName, colName, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(20));
        scan.setFilter(f);
        ResultScanner tx2Scanner = txTable.getScanner(tx2, scan);
        // assertEquals(tx2Scanner.next(100).length, 1);
        Result res = tx2Scanner.next();
        int count20 = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Deleting row id {} with value {}", Bytes.toString(res.getRow()),
                     Bytes.toInt(res.getValue(famName, colName)));
            Delete delete20 = new Delete(res.getRow());
            txTable.delete(tx2, delete20);
            res = tx2Scanner.next();
            count20++;
        }
        assertEquals(count20, 1);
        // 3) commit TX 1
        tm.commit(tx1);

        tx2Scanner = txTable.getScanner(tx2, scan);
        assertNull(tx2Scanner.next());

        // 4) commit TX 2 -> Should be rolled-back
        try {
            tm.commit(tx2);
            fail();
        } catch (RollbackException e) {
            // Expected
        }

    }

    @Test
    public void testSIPreventsLostUpdates(ITestContext context) throws Exception {
        // TX History for P4:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where id = 1; -- T1
        // select * from test where id = 1; -- T2
        // update test set value = 11 where id = 1; -- T1
        // update test set value = 11 where id = 1; -- T2, BLOCKS
        // commit; -- T1. T2 now prints out "ERROR: could not serialize access due to concurrent update"
        // abort;  -- T2. There's nothing else we can do, this transaction has failed

        // 0) Start transactions
        TransactionManager tm = newTransactionManager(context);
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        Scan scan = new Scan(rowId1, rowId1);
        scan.addColumn(famName, colName);

        // 1) select * from test where id = 1; -- T1
        ResultScanner tx1Scanner = txTable.getScanner(tx1, scan);
        Result res = tx1Scanner.next();
        int count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                     Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId1);
            assertEquals(res.getValue(famName, colName), dataValue1);
            res = tx1Scanner.next();
            count++;
        }
        assertEquals(count, 1);

        // 2) select * from test where id = 1; -- T2
        ResultScanner tx2Scanner = txTable.getScanner(tx2, scan);
        res = tx2Scanner.next();
        count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                     Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId1);
            assertEquals(res.getValue(famName, colName), dataValue1);
            res = tx2Scanner.next();
            count++;
        }
        assertEquals(count, 1);

        // 3) update test set value = 11 where id = 1; -- T1
        Put updateRow1Tx1 = new Put(rowId1);
        updateRow1Tx1.add(famName, colName, Bytes.toBytes("11"));
        txTable.put(tx1, updateRow1Tx1);

        // 4) update test set value = 11 where id = 1; -- T2
        Put updateRow1Tx2 = new Put(rowId1);
        updateRow1Tx2.add(famName, colName, Bytes.toBytes("11"));
        txTable.put(tx2, updateRow1Tx2);

        // 5) commit -- T1
        tm.commit(tx1);

        // 6) commit -- T2 --> should be rolled-back
        try {
            tm.commit(tx2);
            fail();
        } catch (RollbackException e) {
            // Expected
        }

    }

    @Test
    public void testSIPreventsReadSkew(ITestContext context) throws Exception {
        // TX History for G-single:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where id = 1; -- T1. Shows 1 => 10
        // select * from test where id = 1; -- T2
        // select * from test where id = 2; -- T2
        // update test set value = 12 where id = 1; -- T2
        // update test set value = 18 where id = 2; -- T2
        // commit; -- T2
        // select * from test where id = 2; -- T1. Shows 2 => 20
        // commit; -- T1

        // 0) Start transactions
        TransactionManager tm = newTransactionManager(context);
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        Scan rowId1Scan = new Scan(rowId1, rowId1);
        rowId1Scan.addColumn(famName, colName);

        // 1) select * from test where id = 1; -- T1. Shows 1 => 10
        ResultScanner tx1Scanner = txTable.getScanner(tx1, rowId1Scan);
        Result res = tx1Scanner.next();
        int count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                     Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId1);
            assertEquals(res.getValue(famName, colName), dataValue1);
            res = tx1Scanner.next();
            count++;
        }
        assertEquals(count, 1);

        // 2) select * from test where id = 1; -- T2
        ResultScanner tx2Scanner = txTable.getScanner(tx2, rowId1Scan);
        res = tx2Scanner.next();
        count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                     Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId1);
            assertEquals(res.getValue(famName, colName), dataValue1);
            res = tx2Scanner.next();
            count++;
        }

        Scan rowId2Scan = new Scan(rowId2, rowId2);
        rowId2Scan.addColumn(famName, colName);

        // 3) select * from test where id = 2; -- T2
        tx2Scanner = txTable.getScanner(tx2, rowId2Scan);
        res = tx2Scanner.next();
        count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                     Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId2);
            assertEquals(res.getValue(famName, colName), dataValue2);
            res = tx2Scanner.next();
            count++;
        }

        // 4) update test set value = 12 where id = 1; -- T2
        Put updateRow1Tx2 = new Put(rowId1);
        updateRow1Tx2.add(famName, colName, Bytes.toBytes("12"));
        txTable.put(tx1, updateRow1Tx2);

        // 5) update test set value = 18 where id = 1; -- T2
        Put updateRow2Tx2 = new Put(rowId2);
        updateRow2Tx2.add(famName, colName, Bytes.toBytes("18"));
        txTable.put(tx2, updateRow2Tx2);

        // 6) commit -- T2
        tm.commit(tx2);

        // 7) select * from test where id = 2; -- T1. Shows 2 => 20
        tx1Scanner = txTable.getScanner(tx1, rowId2Scan);
        res = tx1Scanner.next();
        count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                     Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId2);
            assertEquals(res.getValue(famName, colName), dataValue2);
            res = tx1Scanner.next();
            count++;
        }

        // 8) commit -- T1
        tm.commit(tx1);

    }

    @Test
    public void testSIPreventsReadSkewUsingWritePredicate(ITestContext context) throws Exception {
        // TX History for G-single:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where id = 1; -- T1. Shows 1 => 10
        // select * from test; -- T2
        // update test set value = 12 where id = 1; -- T2
        // update test set value = 18 where id = 2; -- T2
        // commit; -- T2
        // delete from test where value = 20; -- T1. Prints "ERROR: could not serialize access due to concurrent update"
        // abort; -- T1. There's nothing else we can do, this transaction has failed

        // 0) Start transactions
        TransactionManager tm = newTransactionManager(context);
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        // 1) select * from test; -- T1
        assertNumberOfRows(txTable, tx1, 2, new Scan());

        // 2) select * from test; -- T2
        assertNumberOfRows(txTable, tx2, 2, new Scan());

        // 3) update test set value = 12 where id = 1; -- T2
        // 4) update test set value = 18 where id = 2; -- T2
        Put updateRow1Tx2 = new Put(rowId1);
        updateRow1Tx2.add(famName, colName, Bytes.toBytes(12));
        Put updateRow2Tx2 = new Put(rowId2);
        updateRow2Tx2.add(famName, colName, Bytes.toBytes(18));
        txTable.put(tx2, Arrays.asList(updateRow1Tx2, updateRow2Tx2));

        // 5) commit; -- T2
        tm.commit(tx2);

        // 6) delete from test where value = 20; -- T1. Prints
        // "ERROR: could not serialize access due to concurrent update"
        Filter f = new SingleColumnValueFilter(famName, colName, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(20));
        Scan checkFor20 = new Scan();
        checkFor20.setFilter(f);
        ResultScanner checkFor20Scanner = txTable.getScanner(tx1, checkFor20);
        Result res = checkFor20Scanner.next();
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Deleting row id {} with value {}", Bytes.toString(res.getRow()), Bytes.toInt(res.getValue(famName, colName)));
            Delete delete20 = new Delete(res.getRow());
            txTable.delete(tx1, delete20);
            res = checkFor20Scanner.next();
        }

        // 7) abort; -- T1
        try {
            tm.commit(tx1);
            fail("Should be aborted");
        } catch (RollbackException e) {
            // Expected
        }

    }

    // this test shows that Omid does not provide serilizable level of isolation other wise last commit would have failed
    @Test
    public void testSIDoesNotPreventWriteSkew(ITestContext context) throws Exception {
        // TX History for G2-item:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where id in (1,2); -- T1
        // select * from test where id in (1,2); -- T2
        // update test set value = 11 where id = 1; -- T1
        // update test set value = 21 where id = 2; -- T2
        // commit; -- T1
        // commit; -- T2

        // 0) Start transactions
        TransactionManager tm = newTransactionManager(context);
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        Scan rowId12Scan = new Scan(rowId1, rowId3);
        rowId12Scan.addColumn(famName, colName);

        // 1) select * from test where id in (1,2); -- T1
        ResultScanner tx1Scanner = txTable.getScanner(tx1, rowId12Scan);
        Result res = tx1Scanner.next();
        int count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()), Bytes.toInt(res.getValue(famName, colName)));
            switch (count) {
                case 0:
                    assertEquals(res.getRow(), rowId1);
                    assertEquals(res.getValue(famName, colName), dataValue1);
                    break;
                case 1:
                    assertEquals(res.getRow(), rowId2);
                    assertEquals(res.getValue(famName, colName), dataValue2);
                    break;
                default:
                    fail();
            }
            res = tx1Scanner.next();
            count++;
        }
        assertEquals(count, 2);

        // 2) select * from test where id in (1,2); -- T2
        ResultScanner tx2Scanner = txTable.getScanner(tx1, rowId12Scan);
        res = tx2Scanner.next();
        count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()), Bytes.toInt(res.getValue(famName, colName)));
            switch (count) {
                case 0:
                    assertEquals(res.getRow(), rowId1);
                    assertEquals(res.getValue(famName, colName), dataValue1);
                    break;
                case 1:
                    assertEquals(res.getRow(), rowId2);
                    assertEquals(res.getValue(famName, colName), dataValue2);
                    break;
                default:
                    fail();
            }
            res = tx2Scanner.next();
            count++;
        }
        assertEquals(count, 2);

        // 3) update test set value = 11 where id = 1; -- T1
        Put updateRow1Tx1 = new Put(rowId1);
        updateRow1Tx1.add(famName, colName, Bytes.toBytes("11"));
        txTable.put(tx1, updateRow1Tx1);

        // 4) update test set value = 21 where id = 2; -- T2
        Put updateRow2Tx2 = new Put(rowId2);
        updateRow2Tx2.add(famName, colName, Bytes.toBytes("21"));
        txTable.put(tx2, updateRow2Tx2);

        // 5) commit; -- T1
        tm.commit(tx1);

        // 6) commit; -- T2
        tm.commit(tx2);
    }

    // this test shows that Omid does not provide serilizable level of isolation other wise last commit would have failed
    @Test
    public void testSIDoesNotPreventAntiDependencyCycles(ITestContext context) throws Exception {
        // TX History for G2:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where value % 3 = 0; -- T1
        // select * from test where value % 3 = 0; -- T2
        // insert into test (id, value) values(3, 30); -- T1
        // insert into test (id, value) values(4, 42); -- T2
        // commit; -- T1
        // commit; -- T2
        // select * from test where value % 3 = 0; -- Either. Returns 3 => 30, 4 => 42

        // 0) Start transactions
        TransactionManager tm = newTransactionManager(context);
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        Filter f = new SingleColumnValueFilter(famName, colName, CompareFilter.CompareOp.EQUAL, Bytes.toBytes("30"));
        Scan value30 = new Scan();
        value30.setFilter(f);
        value30.addColumn(famName, colName);

        // 1) select * from test where value % 3 = 0; -- T1
        assertNumberOfRows(txTable, tx1, 0, value30);


        // 2) select * from test where value % 3 = 0; -- T2
        assertNumberOfRows(txTable, tx2, 0, value30);


        // 3) insert into test (id, value) values(3, 30); -- T1
        Put insertRow3Tx1 = new Put(rowId1);
        insertRow3Tx1.add(famName, colName, Bytes.toBytes("30"));
        txTable.put(tx1, insertRow3Tx1);

        // 4) insert into test (id, value) values(4, 42); -- T2
        Put updateRow4Tx2 = new Put(rowId2);
        updateRow4Tx2.add(famName, colName, Bytes.toBytes("42"));
        txTable.put(tx2, updateRow4Tx2);

        // 5) commit; -- T1
        tm.commit(tx1);

        // 6) commit; -- T2
        tm.commit(tx2);

        // 7) select * from test where value % 3 = 0; -- Either. Returns 3 => 30, 4 => 42
    }

    /**
     * This translates the table initialization done in:
     * https://github.com/ept/hermitage/blob/master/postgres.md
     *
     * create table test (id int primary key, value int);
     * insert into test (id, value) values (1, 10), (2, 20);
     */
    @BeforeMethod(alwaysRun = true)
    private void loadBaseDataOnTestTable(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);
        TTable txTable = new TTable(hbaseConf, TEST_TABLE);

        Transaction initializationTx = tm.begin();
        Put row1 = new Put(rowId1);
        row1.add(famName, colName, dataValue1);
        txTable.put(initializationTx, row1);
        Put row2 = new Put(rowId2);
        row2.add(famName, colName, dataValue2);
        txTable.put(initializationTx, row2);

        tm.commit(initializationTx);
    }


    private void assertNumberOfRows(TTable txTable, Transaction tx2, int maxCount, Scan scan) throws IOException {
        int count = 0;
        ResultScanner tx2Scanner = txTable.getScanner(tx2, scan);
        Result res = tx2Scanner.next();
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()), Bytes.toInt(res.getValue(famName, colName)));
            res = tx2Scanner.next();
            count++;
        }
        assertEquals(count, maxCount);
    }


}
