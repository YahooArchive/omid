package com.yahoo.omid.transaction;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class HBaseTransaction extends AbstractTransaction<HBaseCellId> {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseTransaction.class);

    HBaseTransaction(long transactionId, long epoch, Set<HBaseCellId> writeSet, AbstractTransactionManager tm) {
        super(transactionId, epoch, writeSet, tm);
    }

    @Override
    public void cleanup() {
        Set<HBaseCellId> writeSet = getWriteSet();
        for (final HBaseCellId cell : writeSet) {
            Delete delete = new Delete(cell.getRow());
            delete.deleteColumn(cell.getFamily(), cell.getQualifier(), getStartTimestamp());
            try {
                cell.getTable().delete(delete);
            } catch (IOException e) {
                LOG.warn("Failed cleanup cell {} for Tx {}. This issue has been ignored", new Object[] { cell, getTransactionId(), e });
            }
        }
        try {
            flushTables();
        } catch (IOException e) {
            LOG.warn("Failed flushing tables for Tx {}", getTransactionId(), e);
        }
    }

    /**
     * Flushes pending operations for tables touched by transaction
     */
    public void flushTables() throws IOException {

        for (HTableInterface writtenTable : getWrittenTables()) {
            writtenTable.flushCommits();
        }

    }

    // ****************************************************************************************************************
    // Helper methods
    // ****************************************************************************************************************

    private Set<HTableInterface> getWrittenTables() {
        HashSet<HBaseCellId> writeSet = (HashSet<HBaseCellId>) getWriteSet();
        Set<HTableInterface> tables = new HashSet<HTableInterface>();
        for (HBaseCellId cell : writeSet) {
            tables.add(cell.getTable());
        }
        return tables;
    }

}
