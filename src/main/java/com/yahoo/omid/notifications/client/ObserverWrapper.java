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

import static com.yahoo.omid.notifications.Constants.HBASE_META_CF;
import static com.yahoo.omid.notifications.Constants.HBASE_NOTIFY_SUFFIX;
import static com.yahoo.omid.notifications.Constants.NOTIFY_FALSE;
import static com.yahoo.omid.notifications.Constants.NOTIFY_TRUE;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.notifications.metrics.ClientSideAppMetrics;
import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.transaction.RollbackException;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;
import com.yammer.metrics.core.TimerContext;

public class ObserverWrapper implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ObserverWrapper.class);

    private Observer observer;

    private Configuration tsoClientHbaseConf;

    private TransactionManager tm;

    private ClientSideAppMetrics metrics;

    private BlockingQueue<Notification> notifQueue;

    private TTable txTable;

    private int id;

    public static final int PULL_TIMEOUT_MS = 100;

    public ObserverWrapper(final Observer observer, ClientSideAppMetrics metrics,
            BlockingQueue<Notification> notifQueue, Configuration configuration, int id) throws IOException {
        this.observer = observer;
        this.metrics = metrics;
        this.notifQueue = notifQueue;
        this.id = id;

        // Configure connection with TSO
        this.tsoClientHbaseConf = configuration;

        try {
            tm = new TransactionManager(tsoClientHbaseConf);
        } catch (Exception e) {
            logger.error("Cannot create transaction manager", e);
            return;
        }
    }

    /**
     * @return the observer's name
     */
    public String getName() {
        return observer.getName();
    }

    @Override
    public void run() {

        try {
            txTable = new TTable(tsoClientHbaseConf, observer.getInterest().getTable());
        } catch (IOException e1) {
            logger.error("cannot connect to table", e1);
            return;
        }

        logger.info("Starting observer wrapper for observer {} for interest {}", observer.getName(), observer
                .getInterest().toStringRepresentation());
        while (!Thread.interrupted()) {
            Notification notification;
            try {
                notification = notifQueue.poll(PULL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                continue;
            }
            if (notification == null) {
                logger.debug("No notification for observer {} after {} ms", observer.getName(),
                        String.valueOf(PULL_TIMEOUT_MS));
                metrics.observerStarvationEvent(observer.getName());
                continue;
            }
            TimerContext timer = metrics.startObserverInvocation(observer.getName());
            notify(observer.getInterest().getTableAsHBaseByteArray(), notification.getRowKey(), observer.getInterest()
                    .getColumnFamilyAsHBaseByteArray(), observer.getInterest().getColumnAsHBaseByteArray());
            timer.stop();
        }
    }

    private void notify(byte[] table, byte[] rowKey, byte[] columnFamily, byte[] column) {
        Transaction tx = null;
        try {
            // Start tx
            tx = tm.begin();
            metrics.observerInvocationEvent(observer.getName());
            Get get = new Get(rowKey);
            Result result = txTable.get(tx, get);
            // Pattern for notify column in framework's metadata column family: <cf>/<c>-notify
            byte[] notifyColumn = Bytes.toBytes(Bytes.toString(columnFamily) + "/" + Bytes.toString(column)
                    + HBASE_NOTIFY_SUFFIX);
            KeyValue notifyValue = result.getColumnLatest(HBASE_META_CF, notifyColumn);
            if (isNotifyFlagSet(notifyValue)) { // Run observer, clear flag and commit transaction
                observer.onInterestChanged(result, tx);
                clearNotifyFlag(tx, txTable, rowKey, notifyColumn);
                tm.commit(tx);
                metrics.observerCompletionEvent(observer.getName());
            } else { // Abort transaction
                tm.rollback(tx);
                metrics.observerAbortEvent(observer.getName());
            }
        } catch (RollbackException e) {
            metrics.omidAbortEvent(observer.getName());
        } catch (IOException e) {
            tm.rollback(tx);
            metrics.hbaseAbortEvent(observer.getName());
            logger.error("Received HBase exception", e);
        } catch (Exception e) {
            metrics.unknownAbortEvent(observer.getName());
            if (tx != null)
                tm.rollback(tx);
            logger.error("Unhandled exception", e);
        }
    }

    private boolean isNotifyFlagSet(KeyValue lastValueNotify) {
        if (lastValueNotify == null) {
            return false;
        }
        byte[] valNotify = lastValueNotify.getValue();
        if (valNotify == null || !Bytes.equals(valNotify, NOTIFY_TRUE)) {
            return false;
        }
        return true;
    }

    /**
     * Clears the notify flag on the corresponding RowKey/Column inside the Omid's transactional context
     * 
     * @param tx
     *            transaction
     * @param tt
     *            transactional table
     * @param rowKey
     * @param notifyColumn
     * @throws IOException
     */
    private void clearNotifyFlag(Transaction tx, TTable tt, byte[] rowKey, byte[] notifyColumn) throws IOException {
        Put put = new Put(rowKey);
        put.add(HBASE_META_CF, notifyColumn, NOTIFY_FALSE);
        tt.put(tx, put);
    }

}
