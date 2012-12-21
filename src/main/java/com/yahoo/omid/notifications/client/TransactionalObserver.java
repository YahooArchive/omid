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
package com.yahoo.omid.notifications.client;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.yahoo.omid.client.TransactionException;
import com.yahoo.omid.client.TransactionManager;
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.client.TransactionalTable;
import com.yahoo.omid.notifications.Constants;
import com.yahoo.omid.notifications.NotificationException;

public class TransactionalObserver {
    
    private static final Logger logger = Logger.getLogger(TransactionalObserver.class);
    
    private String name; // The name of the observer
    private ObserverBehaviour observer;

    private Configuration tsoClientHbaseConf;
    private TransactionManager tm;
    
    public TransactionalObserver(String name, ObserverBehaviour observer) throws Exception{
        this.name = name;
        this.observer = observer;
        
        // Configure connection with TSO
        tsoClientHbaseConf = HBaseConfiguration.create();
        tsoClientHbaseConf.set("tso.host", "localhost");
        tsoClientHbaseConf.setInt("tso.port", 1234);
        
        try {
            tm = new TransactionManager(tsoClientHbaseConf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
    /**
     * @return the transactional observer's name
     */
    public String getName() {
        return name;
    }

    public void notify(byte[] table, byte[] rowKey, byte[] columnFamily, byte[] column) {
        TransactionalTable tt = null;
        TransactionState tx = null;
        try {
            // Start tx
            tt = new TransactionalTable(tsoClientHbaseConf, table);
            // Transaction adding to rows to a table
            tx = tm.beginTransaction();
            checkIfAlreadyExecuted(tx, tt, table, rowKey, columnFamily, column);
            // Perform the particular actions on the observer for this row
            observer.updated(tx, table, rowKey, columnFamily, column);            
            // Commit tx
            tm.tryCommit(tx);
            clearNotifyFlag(table, rowKey, columnFamily, column);
            //logger.trace("TRANSACTION " + tx + " COMMITTED");
        } catch (NotificationException e) {
            //logger.trace("Aborting tx " + tx);
            try { tm.abort(tx); } catch (TransactionException e1) {}
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (tt != null) {
                try { tt.close(); } catch (IOException e) {}
            }
        }
    }

    /**
     * @param tx
     * @param tt 
     * @param column 
     * @param columnFamily 
     * @param rowKey 
     * @param table 
     */
    private void checkIfAlreadyExecuted(TransactionState tx, TransactionalTable tt, byte[] table, byte[] rowKey, byte[] columnFamily, byte[] column) throws Exception {
        String targetColumnFamily = Bytes.toString(columnFamily) + Constants.NOTIF_HBASE_CF_SUFFIX;        
        String targetColumnObserverAck = Bytes.toString(column) + ":" + name;
        String targetColumnNotify = Bytes.toString(column) + ":notify";
        
        //logger.trace("Checking if observer was already executed...");
        Get get = new Get(rowKey);
        Result result = tt.get(tx, get); // Transactional get    
                
        List<KeyValue> listOfObserverAckColumnValues = result.getColumn(Bytes.toBytes(targetColumnFamily), Bytes.toBytes(targetColumnObserverAck));

        KeyValue lastValueAck = null;        
        byte[] valObserverAck = null;
        long tsObserverAck = -1;

        if(listOfObserverAckColumnValues.size() > 0) { // Check this because the observer may have not been initialized yet
            lastValueAck = listOfObserverAckColumnValues.get(0);        
            valObserverAck = lastValueAck.getValue();
            tsObserverAck = lastValueAck.getTimestamp();
        }
                
        List<KeyValue> listOfNotifyColumnValues = result.getColumn(Bytes.toBytes(targetColumnFamily), Bytes.toBytes(targetColumnNotify));
        KeyValue lastValueNotify = listOfNotifyColumnValues.get(0);        
        byte[] valNotify = lastValueNotify.getValue();
        long tsNotify = lastValueNotify.getTimestamp();
        
        //logger.trace("Result :" +  result);
//        logger.trace("TS Notify :" +  tsNotify + " TS Obs Ack " + tsObserverAck);
        if (Bytes.equals(valNotify, Bytes.toBytes("true"))) {
            if (valObserverAck == null || tsObserverAck < tsNotify) { // Si TS notify > TS ack
                // logger.trace("Setting put on observer");
                Put put = new Put(rowKey, tx.getStartTimestamp());
                put.add(Bytes.toBytes(targetColumnFamily), Bytes.toBytes(targetColumnObserverAck), Bytes.toBytes(name));
                tt.put(tx, put); // Transactional put
            } else {
                throw new NotificationException("Observer " + name + " already executed for change");
            }
        } else {
            throw new NotificationException("Notify its not true!!!");
        }
    }

    /**
     * Clears the notify flag on the corresponding RowKey/Column without Omid's transactional context
     * 
     * @param table
     * @param rowKey
     * @param columnFamily
     * @param column
     */
    private void clearNotifyFlag(byte[] table, byte[] rowKey, byte[] columnFamily, byte[] column) {        
        String targetTable = Bytes.toString(table);
        String targetColumnFamily = Bytes.toString(columnFamily) + Constants.NOTIF_HBASE_CF_SUFFIX;        
        String targetColumn = Bytes.toString(column) + Constants.HBASE_NOTIFY_SUFFIX;

        Put put = new Put(rowKey);
        put.add(Bytes.toBytes(targetColumnFamily), Bytes.toBytes(targetColumn), Bytes.toBytes("false"));

        try {
            HTable hTable = new HTable(HBaseConfiguration.create(), targetTable);            
            hTable.put(put);
            //logger.trace("Notify Flag cleared for: " + put);
            hTable.close();
        } catch (Exception e) {
            logger.error("Error clearing Notify Flag for: " + put);
            e.printStackTrace();
        }        
    }
    
}
