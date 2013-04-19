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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;
import com.yahoo.omid.notifications.Constants;
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

    public static final int PULL_TIMEOUT_MS = 100;

    public ObserverWrapper(final Observer observer, String omidHostAndPort, ClientSideAppMetrics metrics,
            BlockingQueue<Notification> notifQueue) throws IOException {
        this.observer = observer;
        final HostAndPort hp = HostAndPort.fromString(omidHostAndPort);
        this.metrics = metrics;
        this.notifQueue = notifQueue;

        // Configure connection with TSO
        tsoClientHbaseConf = HBaseConfiguration.create();
        tsoClientHbaseConf.set("tso.host", hp.getHostText());
        tsoClientHbaseConf.setInt("tso.port", hp.getPort());

        try {
            tm = new TransactionManager(tsoClientHbaseConf);
        } catch (Exception e) {
            logger.error("Cannot create transaction manager", e);
            return;
        }
        txTable = new TTable(tsoClientHbaseConf, observer.getInterest().getTable());

        // logger.info("Instance created for observer " + observer.getName() + " using dispatcher "
        // + getContext().dispatcher() + " Context " + getContext().props());
    }

    /**
     * @return the transactional observer's name
     */
    public String getName() {
        return observer.getName();
    }

    @Override
    public void run() {

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
            // Pattern for notify column in framework's metadata column family: <cf>/<c>-notify
            String notifyColumn = Bytes.toString(columnFamily) + "/" + Bytes.toString(column)
                    + Constants.HBASE_NOTIFY_SUFFIX;
            Get get = new Get(rowKey);
            Result result = txTable.get(tx, get); // Transactional get
            KeyValue notifyValue = result.getColumnLatest(Constants.HBASE_META_CF, Bytes.toBytes(notifyColumn));

            if (isNotifyFlagSet(notifyValue)) {
                // Run observer
                observer.onInterestChanged(result, tx);

                // Clear flag and commit transaction
                clearNotifyFlag(tx, txTable, rowKey, columnFamily, column);
                tm.commit(tx);
                metrics.observerCompletionEvent(observer.getName());
            } else {
                // Abort transaction
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

        if (valNotify == null || !Bytes.equals(valNotify, Bytes.toBytes("true"))) {
            return false;
        }
        return true;
    }

    /**
     * Clears the notify flag on the corresponding RowKey/Column inside the Omid's transactional context
     * 
     * @param table
     * @param rowKey
     * @param columnFamily
     * @param column
     * @throws IOException
     */
    private void clearNotifyFlag(Transaction tx, TTable tt, byte[] rowKey, byte[] columnFamily, byte[] column)
            throws IOException {
        String targetColumn = Bytes.toString(columnFamily) + "/" + Bytes.toString(column)
                + Constants.HBASE_NOTIFY_SUFFIX;

        Put put = new Put(rowKey);
        put.add(Constants.HBASE_META_CF, Bytes.toBytes(targetColumn), Bytes.toBytes("false"));
        tt.put(tx, put); // Transactional put
    }

}
