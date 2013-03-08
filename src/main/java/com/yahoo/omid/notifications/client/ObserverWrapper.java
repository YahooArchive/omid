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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import akka.actor.UntypedActor;

import com.google.common.net.HostAndPort;
import com.yahoo.omid.client.TransactionException;
import com.yahoo.omid.client.TransactionManager;
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.client.TransactionalTable;
import com.yahoo.omid.notifications.Constants;
import com.yahoo.omid.notifications.NotificationException;
import com.yahoo.omid.notifications.metrics.ClientSideAppMetrics;
import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yammer.metrics.core.TimerContext;

public class ObserverWrapper extends UntypedActor {
    
    private static final Logger logger = Logger.getLogger(ObserverWrapper.class);
    
    private Observer observer;

    private Configuration tsoClientHbaseConf;

    private TransactionManager tm;
    
    private ClientSideAppMetrics metrics;
    
    public ObserverWrapper(Observer observer, String omidHostAndPort, ClientSideAppMetrics metrics) {
        this.observer = observer;        
        final HostAndPort hp = HostAndPort.fromString(omidHostAndPort);        
        this.metrics = metrics;
        
        // Configure connection with TSO
        tsoClientHbaseConf = HBaseConfiguration.create();
        tsoClientHbaseConf.set("tso.host", hp.getHostText());
        tsoClientHbaseConf.setInt("tso.port", hp.getPort());
        
        try {
            tm = new TransactionManager(tsoClientHbaseConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.trace("Instance created for observer " + observer.getName());
    }
    
    /**
     * @return the transactional observer's name
     */
    public String getName() {
        return observer.getName();
    }

    /**
     * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
     */
    @Override
    public void onReceive(Object msg) throws Exception {
        if(msg instanceof Notification) {
            Notification notification = (Notification) msg;
            TimerContext timer = metrics.startObserverInvocation(observer.getName());
            notify(notification.getTable(), notification.getRowKey(), notification.getColumnFamily(), notification.getColumn());
            timer.stop();
        } else {
            unhandled(msg);
        }
        
    }
    
    private void notify(byte[] table, byte[] rowKey, byte[] columnFamily, byte[] column) {
        TransactionalTable tt = null;
        TransactionState tx = null;
        try {
            // Start tx
            tt = new TransactionalTable(tsoClientHbaseConf, table);
            // Transaction adding to rows to a table
            tx = tm.beginTransaction();
            metrics.observerInvocationEvent(observer.getName());
            checkIfAlreadyExecuted(tx, tt, rowKey, columnFamily, column);
            // Perform the particular actions on the observer for this row
            observer.onColumnChanged(column, columnFamily, table, rowKey, tx);            
            // Commit tx
            clearNotifyFlag(tx, tt, rowKey, columnFamily, column);
            tm.tryCommit(tx);
            metrics.observerCompletionEvent(observer.getName());
            //logger.trace("TRANSACTION " + tx + " COMMITTED");
        } catch (NotificationException e) {
            //logger.trace("Aborting tx " + tx);
            try { 
                tm.abort(tx); 
                metrics.observerAbortEvent(observer.getName());
            } catch (TransactionException e1) {}
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
    private void checkIfAlreadyExecuted(TransactionState tx, TransactionalTable tt, byte[] rowKey, byte[] columnFamily, byte[] column) throws Exception {
        String targetColumnFamily = Constants.HBASE_META_CF;
        // Pattern for observer column in framework's metadata column family: <cf>/<c>-<obsName>
        String targetColumnObserverAck = Bytes.toString(columnFamily) + "/" + Bytes.toString(column) + "-" + observer.getName();
        // Pattern for notify column in framework's metadata column family: <cf>/<c>-notify
        String targetColumnNotify = Bytes.toString(columnFamily) + "/" + Bytes.toString(column) + Constants.HBASE_NOTIFY_SUFFIX;
        
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
        
        KeyValue lastValueNotify = null;
        byte[] valNotify = null;
        long tsNotify = -1;
        
        if(listOfNotifyColumnValues.size() > 0) {
            lastValueNotify = listOfNotifyColumnValues.get(0);        
            valNotify = lastValueNotify.getValue();
            tsNotify = lastValueNotify.getTimestamp();
        }
        
        // logger.trace("Result :" +  result);
        // logger.trace("TS Notify :" +  tsNotify + " TS Obs Ack " + tsObserverAck);
        if (valNotify != null && Bytes.equals(valNotify, Bytes.toBytes("true"))) {
            // Proceed if TS notify (set by the coprocessor with the TS of the start timestamp of 
            // the transaction) > TS ack set by the last observer executed 
            if (valObserverAck == null || tsObserverAck < tsNotify) { 
                // logger.trace("Setting put on observer");
                Put put = new Put(rowKey, tx.getStartTimestamp());
                put.add(Bytes.toBytes(targetColumnFamily), Bytes.toBytes(targetColumnObserverAck), Bytes.toBytes(observer.getName()));
                tt.put(tx, put); // Transactional put
            } else {
                logger.error("Observer " + observer.getName() + " already executed for change on " + Bytes.toString(columnFamily) + "/"
                        + Bytes.toString(column) + " row " + Bytes.toString(rowKey));
                throw new NotificationException("Observer already executed");
            }
        } else {
            logger.error("Notify its not true!!! So, another notificiation for observer "
                    + observer.getName() + " was previously executed for " + Bytes.toString(columnFamily) + "/"
                    + Bytes.toString(column) + " row " + Bytes.toString(rowKey));
            throw new NotificationException("Notify is not true");
        }
    }

    /**
     * Clears the notify flag on the corresponding RowKey/Column inside the Omid's transactional context
     * 
     * @param table
     * @param rowKey
     * @param columnFamily
     * @param column
     */
    private void clearNotifyFlag(TransactionState tx, TransactionalTable tt, byte[] rowKey, byte[] columnFamily, byte[] column) {        
        String targetColumnFamily = Constants.HBASE_META_CF;        
        String targetColumn = Bytes.toString(columnFamily) + "/" + Bytes.toString(column) + Constants.HBASE_NOTIFY_SUFFIX;

        Put put = new Put(rowKey);
        put.add(Bytes.toBytes(targetColumnFamily), Bytes.toBytes(targetColumn), Bytes.toBytes("false"));
        try {
            tt.put(tx, put); // Transactional put
        } catch (Exception e) {
            logger.error("Error clearing Notify Flag for: " + put);
            e.printStackTrace();
        }        
    }
    
}
