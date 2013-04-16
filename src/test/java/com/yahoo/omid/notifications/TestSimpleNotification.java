/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.yahoo.omid.notifications;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.yahoo.omid.client.CommitUnsuccessfulException;
import com.yahoo.omid.client.TransactionException;
import com.yahoo.omid.client.TransactionManager;
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.client.TransactionalTable;
import com.yahoo.omid.notifications.client.DeltaOmid;
import com.yahoo.omid.notifications.client.IncrementalApplication;
import com.yahoo.omid.notifications.client.Observer;

public class TestSimpleNotification extends TestInfrastructure {

    private static final Log logger = LogFactory.getLog(TestSimpleNotification.class);

    private static final String VAL_1 = "testWrite-1";
    private static final String VAL_2 = "testWrite-2";
    private static final String VAL_3 = "testWrite-3";

    private static long st;

    @Test
    public void testObserverReceivesACorrectNotification() throws Exception {

        final CountDownLatch cdl = new CountDownLatch(1); // # of observers

        Interest interestObs = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        IncrementalApplication app = new DeltaOmid.AppBuilder("testObserverReceivesACorrectNotificationApp", 6666)
                .addObserver(buildPassiveTransactionalObserver("obs", interestObs, VAL_1, cdl)).build();

        Thread.currentThread().sleep(5000);

        startTriggerTransaction(true, "row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        Assert.assertTrue(cdl.await(10, TimeUnit.SECONDS));
        app.close();
    }

    @Test
    public void testThreeChainedObserversReceiveTheCorrectNotifications() throws Exception {

        final CountDownLatch cdl = new CountDownLatch(3); // # of observers

        Interest interestObs1 = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        Interest interestObs2 = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2);
        Interest interestObs3 = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_3);

        IncrementalApplication app = new DeltaOmid.AppBuilder(
                "testThreeChainedObserversReceiveTheCorrectNotificationsApp", 6667)
                .addObserver(
                        buildActiveTransactionalObserver("obs1", interestObs1, VAL_1, 0, "row-2",
                                TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_2, cdl))
                .addObserver(
                        buildActiveTransactionalObserver("obs2", interestObs2, VAL_2, 0, "row-3",
                                TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_3, VAL_3, cdl))
                .addObserver(buildPassiveTransactionalObserver("obs3", interestObs3, VAL_3, cdl)).build();

        Thread.currentThread().sleep(10000);

        startTriggerTransaction(true, "row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        cdl.await();
        app.close();
    }

    @Test
    public void testTxWritesTheRightMetadataOnCommit() throws Exception {

        final CountDownLatch cdl = new CountDownLatch(1); // # of observers

        Observer observer = new Observer() {
            Interest interest = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);

            public void onInterestChanged(Result rowData, TransactionState tx) {
                st = tx.getStartTimestamp();
                Configuration hbaseClientConf = HBaseConfiguration.create();
                try {
                    TransactionalTable tt = new TransactionalTable(hbaseClientConf, interest.getTable());
                    Get row = new Get(rowData.getRow());
                    Result result = tt.get(row);
                    byte[] val = result.getValue(
                            Bytes.toBytes(Constants.HBASE_META_CF),
                            Bytes.toBytes(TestConstants.COLUMN_FAMILY_1 + "/" + TestConstants.COLUMN_1
                                    + Constants.HBASE_NOTIFY_SUFFIX));
                    assertEquals("Values for this row are different", "true", Bytes.toString(val));
                    byte[] rowDataVal = rowData.getValue(
                            Bytes.toBytes(Constants.HBASE_META_CF),
                            Bytes.toBytes(TestConstants.COLUMN_FAMILY_1 + "/" + TestConstants.COLUMN_1
                                    + Constants.HBASE_NOTIFY_SUFFIX));
                    assertEquals("Values for this row are different", "true", Bytes.toString(rowDataVal));
                    tt.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    cdl.countDown();
                }
                cdl.countDown();
            }

            @Override
            public String getName() {
                return "obs";
            }

            @Override
            public Interest getInterest() {
                return interest;
            }
        };

        IncrementalApplication app = new DeltaOmid.AppBuilder("testTxWritesTheRightMetadataOnCommitApp", 6668)
                .addObserver(observer).build();

        Thread.currentThread().sleep(10000); // Let's the application to register itself

        long tst = startTriggerTransaction(true, "row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        cdl.await();

        Thread.currentThread().sleep(10000); // Let's the observer transaction to commit

        // Check all the metadata values
        Configuration hbaseConfig = HBaseConfiguration.create();
        HTable ht = null;
        Result result = null;
        try {
            ht = new HTable(hbaseConfig, TestConstants.TABLE);
            Get row = new Get(Bytes.toBytes("row-1"));
            result = ht.get(row);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            assertNotNull("Result shouldn't be null", result);

            // Check timestamps
            KeyValue[] kvs = result.raw();
            assertEquals("Only 2 KV should be returned", 2, kvs.length);
            assertEquals("Incorrect timestamp", tst, kvs[0].getTimestamp()); // 0 = Value written
            assertEquals("Incorrect timestamp", st, kvs[1].getTimestamp()); // 1 = *-notify meta-column

            byte[] val = null;
            byte[] notifyVal = null;
            val = result.getValue(Bytes.toBytes(TestConstants.COLUMN_FAMILY_1), Bytes.toBytes(TestConstants.COLUMN_1));
            notifyVal = result.getValue(
                    Bytes.toBytes(Constants.HBASE_META_CF),
                    Bytes.toBytes(TestConstants.COLUMN_FAMILY_1 + "/" + TestConstants.COLUMN_1
                            + Constants.HBASE_NOTIFY_SUFFIX));
            assertEquals("Values for this column are different", VAL_1, Bytes.toString(val));
            assertEquals("Values for this column are different", "false", Bytes.toString(notifyVal));
            if (ht != null) {
                ht.close();
            }
        }

        app.close();
    }

    @Test
    public void testFlagCleanedUpAfterAbort() throws Exception {

        IncrementalApplication app = new DeltaOmid.AppBuilder("testTxWritesTheRightMetadataOnAbortApp", 6669)
                .addObserver(new Observer() {
                    @Override
                    public void onInterestChanged(Result rowData, TransactionState tx) {
                    }

                    @Override
                    public String getName() {
                        return "obs1";
                    }

                    @Override
                    public Interest getInterest() {
                        return new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
                    }
                }).build();

        Thread.sleep(10000);

        startTriggerTransaction(false, "row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        // Check all the metadata values
        Configuration hbaseConfig = HBaseConfiguration.create();
        HTable ht = new HTable(hbaseConfig, TestConstants.TABLE);
        Get row = new Get(Bytes.toBytes("row-1"));
        Result result = ht.get(row);

        assertNotNull("Result shouldn't be null", result);

        // Check there are no kvs in the DB
        KeyValue[] kvs = result.raw();
        assertEquals("No KVs should be returned", 0, kvs.length);

        ht.close();
        app.close();
    }

    @Test
    public void testTxWritesTheRightMetadataOnAbort() throws Exception {

        Observer observer = new Observer() {
            Interest interest = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);

            public void onInterestChanged(Result rowData, TransactionState tx) {
                st = tx.getStartTimestamp();
                logger.info("Observer TS: " + KeyValue.humanReadableTimestamp(st));
            }

            @Override
            public String getName() {
                return "obs";
            }

            @Override
            public Interest getInterest() {
                return interest;
            }
        };

        IncrementalApplication app = new DeltaOmid.AppBuilder("testTxWritesTheRightMetadataOnAbortApp", 6669)
                .addObserver(observer).build();

        Thread.currentThread().sleep(10000); // Let's the application to register itself

        long tst = startTriggerTransaction(false, "row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        Thread.currentThread().sleep(10000); // Let's the observer transaction to commit

        // Check all the metadata values
        Configuration hbaseConfig = HBaseConfiguration.create();
        HTable ht = null;
        Result result = null;
        try {
            ht = new HTable(hbaseConfig, TestConstants.TABLE);
            Get row = new Get(Bytes.toBytes("row-1"));
            result = ht.get(row);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            assertNotNull("Result shouldn't be null", result);
            KeyValue[] kvs = result.raw();
            assertEquals("Zero should be returned", 0, kvs.length); // 0 = *-notify meta-column
            if (ht != null) {
                ht.close();
            }
        }
        app.close();
    }

    /**
     * @param commit
     *            if tx must commit or not
     * @param r
     *            Row Key
     * @param cf
     *            Column Familiy
     * @param c
     *            Column
     * @param v
     *            Value
     * @return start timestamp of the triggered transaction
     * @throws TransactionException
     * @throws IOException
     * @throws CommitUnsuccessfulException
     */
    private long startTriggerTransaction(boolean commit, String r, String cf, String c, String v)
            throws TransactionException, IOException, CommitUnsuccessfulException {
        TransactionManager tm = new TransactionManager(hbaseConf);
        TransactionalTable tt = new TransactionalTable(hbaseConf, TestConstants.TABLE);

        TransactionState tx = tm.beginTransaction();
        long st = tx.getStartTimestamp();

        Put row = new Put(Bytes.toBytes(r));
        row.add(Bytes.toBytes(cf), Bytes.toBytes(c), Bytes.toBytes(v));
        tt.put(tx, row);

        if (commit) {
            tm.tryCommit(tx);
        } else {
            tm.abort(tx);
        }

        tt.close();

        return st;
    }

    /**
     * The observer built by this method just checks that the value received when its notified is the expected
     * 
     * @param obsName
     * @param interest
     *            The interest of the observer
     * @param expectedValue
     *            The value that this observer will check
     * @param cdl
     *            Count down latch
     * @return A passive observer
     * @throws Exception
     */
    private Observer buildPassiveTransactionalObserver(final String obsName, final Interest interest,
            final String expectedValue, final CountDownLatch cdl) throws Exception {
        return new Observer() {
            public void onInterestChanged(Result rowData, TransactionState tx) {
                Configuration tsoClientConf = HBaseConfiguration.create();
                tsoClientConf.set("tso.host", "localhost");
                tsoClientConf.setInt("tso.port", 1234);

                try {
                    TransactionalTable tt = new TransactionalTable(tsoClientConf, interest.getTable());
                    Get row = new Get(rowData.getRow());
                    Result result = tt.get(row);
                    byte[] val = result.getValue(interest.getColumnFamilyAsHBaseByteArray(),
                            interest.getColumnAsHBaseByteArray());
                    assertEquals("Values for this row are different", expectedValue, Bytes.toString(val));
                    byte[] rowDataVal = rowData.getValue(interest.getColumnFamilyAsHBaseByteArray(),
                            interest.getColumnAsHBaseByteArray());
                    assertEquals("Values for this row are different", expectedValue, Bytes.toString(rowDataVal));
                    tt.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    cdl.countDown();
                }
            }

            @Override
            public String getName() {
                return obsName;
            }

            @Override
            public Interest getInterest() {
                return interest;
            }
        };
    }

    /**
     * The observer built by this method checks that the value received when its notified is the expected and generates
     * a new value on a row as part of the transactional context it has.
     * 
     * @param obsName
     * @param interest
     *            The interest of the observer
     * @param expectedValue
     *            The value that this observer will check
     * @param r
     *            Row Key to write in
     * @param cf
     *            Column Family to write in
     * @param c
     *            Column to write in
     * @param v
     *            Value to write
     * @param cdl
     *            Count down latch
     * @return An active observer
     * @throws Exception
     */
    private Observer buildActiveTransactionalObserver(final String obsName, final Interest interest,
            final String expectedValue, final int delay, final String r, final String cf, final String c,
            final String v, final CountDownLatch cdl) throws Exception {
        return new Observer() {
            public void onInterestChanged(Result rowData, TransactionState tx) {
                if (delay != 0) {
                    try {
                        logger.info("Waitingggggggg");
                        Thread.currentThread().sleep(delay);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
                logger.info("Running to write " + v);
                Configuration tsoClientConf = HBaseConfiguration.create();
                tsoClientConf.set("tso.host", "localhost");
                tsoClientConf.setInt("tso.port", 1234);

                try {
                    TransactionalTable tt = new TransactionalTable(tsoClientConf, interest.getTable());
                    Get row = new Get(rowData.getRow());
                    Result result = tt.get(row);
                    byte[] val = result.getValue(interest.getColumnFamilyAsHBaseByteArray(),
                            interest.getColumnAsHBaseByteArray());
                    assertEquals("Values for this row are different", expectedValue, Bytes.toString(val));
                    byte[] rowDataVal = rowData.getValue(interest.getColumnFamilyAsHBaseByteArray(),
                            interest.getColumnAsHBaseByteArray());
                    assertEquals("Values for this row are different", expectedValue, Bytes.toString(rowDataVal));
                    doTransactionalPut(tx, tt, Bytes.toBytes(r), Bytes.toBytes(cf), Bytes.toBytes(c), Bytes.toBytes(v));
                    tt.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    cdl.countDown();
                }
            }

            @Override
            public String getName() {
                return obsName;
            }

            @Override
            public Interest getInterest() {
                return interest;
            }
        };
    }

    private static void doTransactionalPut(TransactionState tx, TransactionalTable tt, byte[] rowKey,
            byte[] colFamName, byte[] colName, byte[] dataValue) throws IOException {
        Put row = new Put(rowKey);
        row.add(colFamName, colName, dataValue);
        tt.put(tx, row);
    }
}