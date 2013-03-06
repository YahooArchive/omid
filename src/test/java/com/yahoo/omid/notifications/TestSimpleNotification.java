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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
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
    
    private static final String VAL_1= "testWrite-1";
    private static final String VAL_2= "testWrite-2";
    private static final String VAL_3= "testWrite-3";
    
    @Test
    public void testObserverReceivesACorrectNotification() throws Exception {

        final CountDownLatch cdl = new CountDownLatch(1); // # of observers
        
        Interest interestObs = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666)
                                        .addObserver(buildPassiveTransactionalObserver("obs", interestObs, VAL_1, cdl))
                                        .build();
        
        Thread.currentThread().sleep(5000);

        startTriggerTransaction("row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        cdl.await();
        app.close();
    }
    
    @Test
    public void testTwoObserversWithTheSameInterestReceiveACorrectNotification() throws Exception {

        final CountDownLatch cdl = new CountDownLatch(2); // # of observers

        Interest interestObs = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666)
                                        .addObserver(buildPassiveTransactionalObserver("obs1", interestObs, VAL_1, cdl))
                                        .addObserver(buildPassiveTransactionalObserver("obs2", interestObs, VAL_1, cdl))
                                        .build();
                
        Thread.currentThread().sleep(5000);

        startTriggerTransaction("row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        cdl.await();
        app.close();
    }

    @Test
    public void testTwoObserversWithTheSameInterestReceiveACorrectNotificationAndCanWriteInTheSameColumnValueOfTwoDifferentRows() throws Exception {

        final CountDownLatch cdl = new CountDownLatch(2); // # of observers
        
        Interest interestObs = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        
        IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666)
                                        .addObserver(buildActiveTransactionalObserver("obs1", interestObs, VAL_1, 0, "row-2", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_2, cdl))
                                        .addObserver(buildActiveTransactionalObserver("obs2", interestObs, VAL_1, 0, "row-3", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_3, cdl))
                                        .build();
        
        Thread.currentThread().sleep(5000);

        startTriggerTransaction("row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        cdl.await();
        app.close();
    }

    @Test
    public void testTwoObserversWithTheSameInterestReceiveACorrectNotificationAndOneOfThemAbortsBecauseCannotWriteInTheSameColumnValueOfTheSameRow() throws Exception {
        final CountDownLatch cdl = new CountDownLatch(2); // # of observers
        
        Interest interestObs = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        
        IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666)
                                        .addObserver(buildActiveTransactionalObserver("obs1", interestObs, VAL_1, 0, "row-2", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_2, cdl))
                                        .addObserver(buildActiveTransactionalObserver("obs2", interestObs, VAL_1, 0, "row-2", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_3, cdl))
                                        .build();
        
        Thread.currentThread().sleep(5000);

        startTriggerTransaction("row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        Configuration tsoClientConf = HBaseConfiguration.create();
        tsoClientConf.set("tso.host", "localhost");
        tsoClientConf.setInt("tso.port", 1234);
        
        Thread.currentThread().sleep(10000);
        
        // Check number of versions in Column 2 of Row 2
        TransactionalTable tt = new TransactionalTable(tsoClientConf, TestConstants.TABLE);
        Get row = new Get(Bytes.toBytes("row-2")).setMaxVersions();
        Result result = tt.get(row);
        List<KeyValue> kvl = result.getColumn(Bytes.toBytes(TestConstants.COLUMN_FAMILY_1),
                Bytes.toBytes(TestConstants.COLUMN_2));
        assertEquals("Only one KeyValue should appear on list", 1, kvl.size());
        for(KeyValue kv : kvl) {
            logger.info("TS " + kv.getTimestamp() + " VAL " + new String(kv.getValue()));
        }
        tt.close();
        
        cdl.await();

    }
    
    @Test
    public void testTwoObserversWithTheSameInterestReceiveACorrectNotificationAndOneOfThemAbortsBecauseCannotWriteInTwoDifferentColumnsOfTheSameRow() throws Exception {
        final CountDownLatch cdl = new CountDownLatch(2); // # of observers
        
        Interest interestObs = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);

        IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666)
                                        .addObserver(buildActiveTransactionalObserver("obs1", interestObs, VAL_1, 0, "row-2", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_2, cdl))
                                        .addObserver(buildActiveTransactionalObserver("obs2", interestObs, VAL_1, 0, "row-2", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_3, VAL_3, cdl))
                                        .build();
        
        Thread.currentThread().sleep(5000);

        startTriggerTransaction("row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        Configuration tsoClientConf = HBaseConfiguration.create();
        tsoClientConf.set("tso.host", "localhost");
        tsoClientConf.setInt("tso.port", 1234);
        
        Thread.currentThread().sleep(10000);
        
        // Check number of versions in Column 2 of Row 2
        TransactionalTable tt = new TransactionalTable(tsoClientConf, TestConstants.TABLE);
        Get row = new Get(Bytes.toBytes("row-2")).setMaxVersions();
        Result result = tt.get(row);
        List<KeyValue> kvlc2 = result.getColumn(Bytes.toBytes(TestConstants.COLUMN_FAMILY_1),
                Bytes.toBytes(TestConstants.COLUMN_2));
        List<KeyValue> kvlc3 = result.getColumn(Bytes.toBytes(TestConstants.COLUMN_FAMILY_1),
                Bytes.toBytes(TestConstants.COLUMN_3));
        if(kvlc2.size() != 0) {
            assertEquals("Only one KeyValue should appear on list", 1, kvlc2.size());
            assertEquals("No KeyValue should appear on list", 0, kvlc3.size());
        } else {
            assertEquals("No KeyValue should appear on list", 0, kvlc2.size());
            assertEquals("Only one KeyValue should appear on list", 1, kvlc3.size());
        }

        tt.close();
        
        cdl.await();

    }
    
    @Test
    public void testThreeChainedObserversReceiveTheCorrectNotifications() throws Exception {

        final CountDownLatch cdl = new CountDownLatch(3); // # of observers
        
        Interest interestObs1 = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        Interest interestObs2 = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2);
        Interest interestObs3 = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_3);
        
        IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666)
                                    .addObserver(buildActiveTransactionalObserver("obs1", interestObs1, VAL_1, 0, "row-2", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_2, cdl))
                                    .addObserver(buildActiveTransactionalObserver("obs2", interestObs2, VAL_2, 0, "row-3", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_3, VAL_3, cdl))
                                    .addObserver(buildPassiveTransactionalObserver("obs3", interestObs3, VAL_3, cdl))
                                    .build();
        
        Thread.currentThread().sleep(5000);        

        startTriggerTransaction("row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        cdl.await();
    }

    /**
     * @param r Row Key
     * @param cf Column Familiy
     * @param c Column
     * @param v Value
     * @throws TransactionException
     * @throws IOException
     * @throws CommitUnsuccessfulException
     */
    private void startTriggerTransaction(String r, String cf, String c, String v) throws TransactionException, IOException, CommitUnsuccessfulException {
        TransactionManager tm = new TransactionManager(hbaseConf);
        TransactionalTable tt = new TransactionalTable(hbaseConf, TestConstants.TABLE);

        TransactionState tx = tm.beginTransaction();

        Put row = new Put(Bytes.toBytes(r));
        row.add(Bytes.toBytes(cf), Bytes.toBytes(c),
                Bytes.toBytes(v));
        tt.put(tx, row);
        
        tm.tryCommit(tx);
        
        tt.close();
    }
    
    /**
     * The observer built by this method just checks that the value received when its notified is the expected
     * 
     * @param obsName
     * @param interest The interest of the observer
     * @param expectedValue The value that this observer will check 
     * @param cdl Count down latch
     * @return A passive observer
     * @throws Exception
     */
    private Observer buildPassiveTransactionalObserver(final String obsName, final Interest interest, final String expectedValue, final CountDownLatch cdl) throws Exception {
        return new Observer() {
            public void onColumnChanged(byte[] column, byte[] columnFamily, byte[] table, byte[] rowKey, TransactionState tx) {
                Configuration tsoClientConf = HBaseConfiguration.create();
                tsoClientConf.set("tso.host", "localhost");
                tsoClientConf.setInt("tso.port", 1234);
                
                try {
                    TransactionalTable tt = new TransactionalTable(tsoClientConf, Bytes.toString(table));
                    Get row = new Get(rowKey);
                    Result result = tt.get(row);
                    byte[] val = result.getValue(columnFamily, column);
                    assertEquals("Values for this row are different", expectedValue, Bytes.toString(val));
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
            public List<Interest> getInterests() {                
                return Collections.singletonList(interest);
            }
        };
    }

    /**
     * The observer built by this method checks that the value received when its notified is the expected and generates
     * a new value on a row as part of the transactional context it has.
     * 
     * @param obsName
     * @param interest The interest of the observer
     * @param expectedValue The value that this observer will check 
     * @param r Row Key to write in
     * @param cf Column Family to write in
     * @param c Column to write in
     * @param v Value to write
     * @param cdl Count down latch
     * @return An active observer
     * @throws Exception
     */
    private Observer buildActiveTransactionalObserver(final String obsName, final Interest interest,
            final String expectedValue, final int delay, final String r, final String cf, final String c,
            final String v, final CountDownLatch cdl) throws Exception {
        return new Observer() {
            public void onColumnChanged(byte[] column, byte[] columnFamily, byte[] table, byte[] rowKey, TransactionState tx) {
                if(delay != 0) {
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
                    TransactionalTable tt = new TransactionalTable(tsoClientConf, Bytes.toString(table));
                    Get row = new Get(rowKey);
                    Result result = tt.get(row);
                    byte[] val = result.getValue(columnFamily, column);
                    assertEquals("Values for this row are different", expectedValue, Bytes.toString(val));                    
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
            public List<Interest> getInterests() {
                return Collections.singletonList(interest);
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