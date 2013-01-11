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
import com.yahoo.omid.notifications.client.ObserverBehaviour;
import com.yahoo.omid.notifications.client.TransactionalObserver;

public class TestSimpleNotification extends TestInfrastructure {

    private static final Log logger = LogFactory.getLog(TestSimpleNotification.class);
    
    private static final String VAL_1= "testWrite-1";
    private static final String VAL_2= "testWrite-2";
    private static final String VAL_3= "testWrite-3";
    
    @Test
    public void testObserverReceivesACorrectNotification() throws Exception {

        final CountDownLatch cdl = new CountDownLatch(1); // # of observers

        TransactionalObserver obs = buildPassiveTransactionalObserver("obs", VAL_1, cdl);
        
        Interest interestObs = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        registrationService.register(obs, interestObs);
        
        Thread.currentThread().sleep(5000);

        startTriggerTransaction("row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        cdl.await();
    }
    
    @Test
    public void testTwoObserversWithTheSameInterestReceiveACorrectNotification() throws Exception {

        final CountDownLatch cdl = new CountDownLatch(2); // # of observers

        TransactionalObserver obs1 = buildPassiveTransactionalObserver("obs1", VAL_1, cdl);
        TransactionalObserver obs2 = buildPassiveTransactionalObserver("obs2", VAL_1, cdl);
        
        Interest interestObs = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        registrationService.register(obs1, interestObs);
        registrationService.register(obs2, interestObs);
        
        Thread.currentThread().sleep(5000);

        startTriggerTransaction("row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        cdl.await();
    }

    @Test
    public void testTwoObserversWithTheSameInterestReceiveACorrectNotificationAndCanWriteInTheSameColumnValueOfTwoDifferentRows() throws Exception {

        final CountDownLatch cdl = new CountDownLatch(2); // # of observers

        TransactionalObserver obs1 = buildActiveTransactionalObserver("obs1", VAL_1, 0, "row-2", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_2, cdl);
        TransactionalObserver obs2 = buildActiveTransactionalObserver("obs2", VAL_1, 0, "row-3", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_3, cdl);
        
        Interest interestObs = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        registrationService.register(obs1, interestObs);
        registrationService.register(obs2, interestObs);
        
        Thread.currentThread().sleep(5000);

        startTriggerTransaction("row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        cdl.await();
    }

    // TODO Check this test when TransactionalObserver is implemented asynchronously
    @Test
    public void testTwoObserversWithTheSameInterestReceiveACorrectNotificationAndNoneOfThemAbortsDespiteWritingTheSameColumnValueOfTheSameRowBecauseTheyAreExecutedSerially() throws Exception {

        final CountDownLatch cdl = new CountDownLatch(2); // # of observers
        
        TransactionalObserver obs1 = buildActiveTransactionalObserver("obs1", VAL_1, 0, "row-2", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_2, cdl);
        TransactionalObserver obs2 = buildActiveTransactionalObserver("obs2", VAL_1, 0, "row-2", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_3, cdl);
        
        Interest interestObs = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        registrationService.register(obs1, interestObs);
        registrationService.register(obs2, interestObs);
        
        Thread.currentThread().sleep(5000);

        startTriggerTransaction("row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);
        
        cdl.await();
    }

    @Test(expected=CommitUnsuccessfulException.class)
    public void testTwoObserversWithTheSameInterestReceiveACorrectNotificationAndOneOfThemAbortsBecauseCannotWriteInTheSameColumnValueOfTheSameRow() throws Exception {
        final CountDownLatch cdl = new CountDownLatch(2); // # of observers
        // TODO It won't work till TransactionalObservers are asynchronous
        TransactionalObserver obs1 = buildActiveTransactionalObserver("obs1", VAL_1, 0, "row-2", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_2, cdl);
        TransactionalObserver obs2 = buildActiveTransactionalObserver("obs2", VAL_1, 0, "row-2", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_3, cdl);
        
        Interest interestObs = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        registrationService.register(obs1, interestObs);
        registrationService.register(obs2, interestObs);
        
        Thread.currentThread().sleep(5000);

        startTriggerTransaction("row-1", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1, VAL_1);

        Configuration tsoClientConf = HBaseConfiguration.create();
        tsoClientConf.set("tso.host", "localhost");
        tsoClientConf.setInt("tso.port", 1234);
        
        Thread.currentThread().sleep(10000);
        
        // Print versions
        TransactionalTable tt = new TransactionalTable(tsoClientConf, TestConstants.TABLE);
        Get row = new Get(Bytes.toBytes("row-2")).setMaxVersions();
        Result result = tt.get(row);
        List<KeyValue> kvl = result.getColumn(Bytes.toBytes(TestConstants.COLUMN_FAMILY_1),
                Bytes.toBytes(TestConstants.COLUMN_2));
        for(KeyValue kv : kvl) {
            logger.info("TS " + kv.getTimestamp() + " VAL " + new String(kv.getValue()));
        }
        tt.close();
        
        cdl.await();

    }
    
    @Test
    public void testThreeChainedObserversReceiveTheCorrectNotifications() throws Exception {

        final CountDownLatch cdl = new CountDownLatch(3); // # of observers
        
        TransactionalObserver obs1 = buildActiveTransactionalObserver("obs1", VAL_1, 0, "row-2", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2, VAL_2, cdl);
        TransactionalObserver obs2 = buildActiveTransactionalObserver("obs2", VAL_2, 0, "row-3", TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_3, VAL_3, cdl);
        TransactionalObserver obs3 = buildPassiveTransactionalObserver("obs3", VAL_3, cdl);
        
        Interest interestObs1 = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_1);
        Interest interestObs2 = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_2);
        Interest interestObs3 = new Interest(TestConstants.TABLE, TestConstants.COLUMN_FAMILY_1, TestConstants.COLUMN_3);
        
        registrationService.register(obs1, interestObs1);
        registrationService.register(obs2, interestObs2);
        registrationService.register(obs3, interestObs3);
        
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
     * @param obsName The name of the generated observer
     * @param expectedValue The value that this observer will check 
     * @param cdl Count down latch
     * @return A passive observer
     * @throws Exception
     */
    private TransactionalObserver buildPassiveTransactionalObserver(String obsName, final String expectedValue, final CountDownLatch cdl) throws Exception {
        return new TransactionalObserver(obsName, new ObserverBehaviour() {
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
        });
    }

    /**
     * The observer built by this method checks that the value received when its notified is the expected and generates
     * a new value on a row as part of the transactional context it has.
     * 
     * @param obsName The name of the generated observer
     * @param expectedValue The value that this observer will check 
     * @param r Row Key to write in
     * @param cf Column Family to write in
     * @param c Column to write in
     * @param v Value to write
     * @param cdl Count down latch
     * @return An active observer
     * @throws Exception
     */
    private TransactionalObserver buildActiveTransactionalObserver(String obsName, final String expectedValue, final int delay,
            final String r, final String cf, final String c, final String v, final CountDownLatch cdl) throws Exception {
        return new TransactionalObserver(obsName, new ObserverBehaviour() {
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
        });
    }
    
    private static void doTransactionalPut(TransactionState tx, TransactionalTable tt, byte[] rowKey,
            byte[] colFamName, byte[] colName, byte[] dataValue) throws IOException {
        Put row = new Put(rowKey);
        row.add(colFamName, colName, dataValue);
        tt.put(tx, row);
    }
}