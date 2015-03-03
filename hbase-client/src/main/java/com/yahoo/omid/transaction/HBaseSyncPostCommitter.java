/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.transaction;

import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.Timer;
import com.yahoo.omid.tsoclient.CellId;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.yahoo.omid.metrics.MetricsUtils.name;

public class HBaseSyncPostCommitter implements PostCommitActions {

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
    public void updateShadowCells(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException
    {

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
                    throw new TransactionManagerException(tx + ": Error inserting shadow cell " + cell, e);
                }
            }

            // Flush affected tables before returning to avoid loss of shadow cells updates when autoflush is disabled
            try {
                tx.flushTables();
            } catch (IOException e) {
                throw new TransactionManagerException(tx + ": Error while flushing writes", e);
            }

        } finally {
            shadowCellsUpdateTimer.stop();
        }

    }

    @Override
    public void removeCommitTableEntry(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException
    {

        HBaseTransaction tx = HBaseTransactionManager.enforceHBaseTransactionAsParam(transaction);

        commitTableUpdateTimer.start();

        try {
            commitTableClient.completeTransaction(tx.getStartTimestamp()).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransactionManagerException(tx + ": interrupted during commit table entry delete");
        } catch (ExecutionException e) {
            throw new TransactionManagerException(tx + ": can't remove commit table entry");
        } finally {
            commitTableUpdateTimer.start();
        }

    }

    @Override
    public void updateShadowCellsAndRemoveCommitTableEntry(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException
    {
        updateShadowCells(transaction);
        removeCommitTableEntry(transaction);
    }

}
