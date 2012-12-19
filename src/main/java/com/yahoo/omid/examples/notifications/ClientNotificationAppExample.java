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
package com.yahoo.omid.examples.notifications;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.yahoo.omid.client.TransactionManager;
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.client.TransactionalTable;
import com.yahoo.omid.examples.Constants;
import com.yahoo.omid.notifications.Interest;
import com.yahoo.omid.notifications.client.ObserverBehaviour;
import com.yahoo.omid.notifications.client.ObserverRegistrationService;
import com.yahoo.omid.notifications.client.TransactionalObserver;

/**
 * This applications shows the basic usage of the Omid's notification framework
 * 
 */
public class ClientNotificationAppExample {

    private static final Logger logger = Logger.getLogger(ClientNotificationAppExample.class);

    /**
     * Launches ObserverRegistrationService and perform an observer registration
     * 
     * @param args
     */
    public static void main(String[] args) throws Exception {
        final ObserverRegistrationService registrationService = new ObserverRegistrationService();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                registrationService.stopAndWait();
                logger.info("ooo Omid ooo - Omid's Notification Example App Stopped (CTRL+C) - ooo Omid ooo");
            }
        });

        // TSO Client setup
        Configuration tsoClientHbaseConf = HBaseConfiguration.create();
        tsoClientHbaseConf.set("tso.host", "localhost");
        tsoClientHbaseConf.setInt("tso.port", 1234);

        logger.info("ooo Omid ooo - STARTING OMID'S EXAMPLE NOTIFICATION APP. - ooo Omid ooo");
        
        logger.info("ooo Omid ooo -" +
        		" A table called " 
                + Constants.TABLE
                + " with a column Family " 
                + Constants.COLUMN_FAMILY_1 
                + " has been already created by the Omid Infrastructure " +
                "- ooo Omid ooo");

        registrationService.startAndWait();

        TransactionalObserver obs1 = new TransactionalObserver("o1" /* Observer Name */, new ObserverBehaviour() {
            public void updated(TransactionState tx, byte[] table, byte[] rowKey, byte[] columnFamily, byte[] column) {
                logger.info("ooo Omid ooo -"
                        + "I'M OBSERVER o1."
                        + " An update has occurred on Table: "
                        + Bytes.toString(table)
                        + " RowKey: "
                        + Bytes.toString(rowKey)
                        + " ColumnFamily: " 
                        + Bytes.toString(columnFamily)
                        + " Column: " 
                        + Bytes.toString(column)
                        + " !!! - ooo Omid ooo");
                logger.info("ooo Omid ooo - OBSERVER o1 INSERTING A NEW ROW ON COLUMN " + Constants.COLUMN_2 + " UNDER TRANSACTIONAL CONTEXT " + tx + " - ooo Omid ooo");
                Configuration tsoClientConf = HBaseConfiguration.create();
                tsoClientConf.set("tso.host", "localhost");
                tsoClientConf.setInt("tso.port", 1234);
                
                try {
                    TransactionalTable tt = new TransactionalTable(tsoClientConf, Constants.TABLE);
                    doTransactionalPut(tx, tt, rowKey, Bytes.toBytes(Constants.COLUMN_FAMILY_1),
                            Bytes.toBytes(Constants.COLUMN_2), Bytes.toBytes("Data written by OBSERVER o1"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        
        TransactionalObserver obs2 = new TransactionalObserver("o2" /* Observer Name */, new ObserverBehaviour() {
            public void updated(TransactionState tx, byte[] table, byte[] rowKey, byte[] columnFamily, byte[] column) {
                logger.info("ooo Omid ooo - " 
                        + "I'M OBSERVER o2." 
                        + " An update has occurred on Table: "
                        + Bytes.toString(table)
                        + " RowKey: "
                        + Bytes.toString(rowKey)
                        + " ColumnFamily: " 
                        + Bytes.toString(columnFamily)
                        + " Column: " 
                        + Bytes.toString(column)
                        + " !!! I'M NOT GONNA DO ANYTHING ELSE - ooo Omid ooo");           
            }
        });
        
        Interest interestObs1 = new Interest(Constants.TABLE, Constants.COLUMN_FAMILY_1, Constants.COLUMN_1);
        registrationService.register(obs1, interestObs1);
        
        Interest interestObs2 = new Interest(Constants.TABLE, Constants.COLUMN_FAMILY_1, Constants.COLUMN_2);
        registrationService.register(obs2, interestObs2);

        logger.info("ooo Omid ooo - WAITING 10 SECONDS TO ALLOW OBSERVER REGISTRATION - ooo Omid ooo");
        Thread.currentThread().sleep(10000);
        //registrationService.deregister(obs1, interest);
        
        TransactionManager tm = new TransactionManager(tsoClientHbaseConf);
        TransactionalTable tt = new TransactionalTable(tsoClientHbaseConf, Constants.TABLE);
        
        // Transaction adding to rows to a table
        TransactionState tx1 = tm.beginTransaction();
        logger.info("ooo Omid ooo - STARTING TRIGGER TX " + tx1 + " INSERTING TWO ROWS IN COLUMN " + Constants.COLUMN_1 + " - ooo Omid ooo");
        doTransactionalPut(tx1, tt, Bytes.toBytes("row1"), Bytes.toBytes(Constants.COLUMN_FAMILY_1),
                Bytes.toBytes(Constants.COLUMN_1), Bytes.toBytes("testWrite-1"));
        doTransactionalPut(tx1, tt, Bytes.toBytes("row2"), Bytes.toBytes(Constants.COLUMN_FAMILY_1),
                Bytes.toBytes(Constants.COLUMN_1), Bytes.toBytes("testWrite-2"));
        doTransactionalPut(tx1, tt, Bytes.toBytes("row3"), Bytes.toBytes(Constants.COLUMN_FAMILY_1),
                Bytes.toBytes(Constants.COLUMN_1), Bytes.toBytes("testWrite-3"));

        tm.tryCommit(tx1);
        logger.info("ooo Omid ooo - TRIGGER TX " + tx1 + " COMMITTED - ooo Omid ooo");
        tt.close();

        logger.info("ooo Omid ooo - WAITING 15 SECONDS TO ALLOW THE OBSERVER RECEIVING NOTIFICATIONS - ooo Omid ooo");
        Thread.currentThread().sleep(15000);
        
        registrationService.stopAndWait();

        logger.info("ooo Omid ooo - OMID'S NOTIFICATION APP FINISHED - ooo Omid ooo");

    }
    
    private static void doTransactionalPut(TransactionState tx, TransactionalTable tt, byte[] rowName,
            byte[] colFamName, byte[] colName, byte[] dataValue) throws IOException {
        Put row = new Put(rowName);
        row.add(colFamName, colName, dataValue);
        tt.put(tx, row);
    }
    
}
