/**
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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.Timer;
import org.apache.omid.tso.client.CellId;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.apache.omid.metrics.MetricsUtils.name;

public class HBaseSyncPostCommitter implements PostCommitActions {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSyncPostCommitter.class);

    private final MetricsRegistry metrics;
    private final CommitTable.Client commitTableClient;

    private final Timer commitTableUpdateTimer;
    private final Timer shadowCellsUpdateTimer;

    public HBaseSyncPostCommitter(MetricsRegistry metrics, CommitTable.Client commitTableClient) {
        this.metrics = metrics;
        this.commitTableClient = commitTableClient;

        this.commitTableUpdateTimer = metrics.timer(name("omid", "tm", "hbase", "commitTableUpdate", "latency"));
        this.shadowCellsUpdateTimer = metrics.timer(name("omid", "tm", "hbase", "shadowCellsUpdate", "latency"));
    }

    @Override
    public ListenableFuture<Void> updateShadowCells(AbstractTransaction<? extends CellId> transaction) {

        SettableFuture<Void> updateSCFuture = SettableFuture.create();

        HBaseTransaction tx = HBaseTransactionManager.enforceHBaseTransactionAsParam(transaction);

        shadowCellsUpdateTimer.start();
        try {

            // Add shadow cells
            for (HBaseCellId cell : tx.getWriteSet()) {
                Put put = new Put(cell.getRow());
                put.add(cell.getFamily(),
                        CellUtils.addShadowCellSuffix(cell.getQualifier(), 0, cell.getQualifier().length),
                        tx.getStartTimestamp(),
                        Bytes.toBytes(tx.getCommitTimestamp()));
                try {
                    cell.getTable().put(put);
                } catch (IOException e) {
                    LOG.warn("{}: Error inserting shadow cell {}", tx, cell, e);
                    updateSCFuture.setException(
                            new TransactionManagerException(tx + ": Error inserting shadow cell " + cell, e));
                }
            }

            // Flush affected tables before returning to avoid loss of shadow cells updates when autoflush is disabled
            try {
                tx.flushTables();
                updateSCFuture.set(null);
            } catch (IOException e) {
                LOG.warn("{}: Error while flushing writes", tx, e);
                updateSCFuture.setException(new TransactionManagerException(tx + ": Error while flushing writes", e));
            }

        } finally {
            shadowCellsUpdateTimer.stop();
        }

        return updateSCFuture;

    }

    @Override
    public ListenableFuture<Void> removeCommitTableEntry(AbstractTransaction<? extends CellId> transaction) {

        SettableFuture<Void> updateSCFuture = SettableFuture.create();

        HBaseTransaction tx = HBaseTransactionManager.enforceHBaseTransactionAsParam(transaction);

        commitTableUpdateTimer.start();

        try {
            commitTableClient.completeTransaction(tx.getStartTimestamp()).get();
            updateSCFuture.set(null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("{}: interrupted during commit table entry delete", tx, e);
            updateSCFuture.setException(
                    new TransactionManagerException(tx + ": interrupted during commit table entry delete"));
        } catch (ExecutionException e) {
            LOG.warn("{}: can't remove commit table entry", tx, e);
            updateSCFuture.setException(new TransactionManagerException(tx + ": can't remove commit table entry"));
        } finally {
            commitTableUpdateTimer.stop();
        }

        return updateSCFuture;

    }

}
