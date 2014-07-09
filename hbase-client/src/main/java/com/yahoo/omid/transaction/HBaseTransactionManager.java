package com.yahoo.omid.transaction;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTableConfig;
import com.yahoo.omid.tsoclient.CellId;
import com.yahoo.omid.tsoclient.TSOClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class HBaseTransactionManager extends AbstractTransactionManager implements HBaseTransactionClient {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseTransactionManager.class);

    public static final byte[] SHADOW_CELL_SUFFIX = ":OMID_CTS".getBytes(Charsets.UTF_8);

    static class HBaseTransaction extends AbstractTransaction<HBaseCellId> {

        HBaseTransaction(long transactionId, Set<HBaseCellId> writeSet, AbstractTransactionManager tm) {
            super(transactionId, writeSet, tm);
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
                    LOG.warn("Failed cleanup cell {} for Tx {}", new Object[]{cell, getTransactionId(), e});
                }
            }
        }

        public Set<HTableInterface> getWrittenTables() {
            HashSet<HBaseCellId> writeSet = (HashSet<HBaseCellId>) getWriteSet();
            Set<HTableInterface> tables = new HashSet<HTableInterface>();
            for (HBaseCellId cell : writeSet) {
                tables.add(cell.getTable());
            }
            return tables;
        }

    }

    private static class HBaseTransactionFactory implements TransactionFactory<HBaseCellId> {

        @Override
        public HBaseTransaction createTransaction(
                long transactionId, AbstractTransactionManager tm) {

            return new HBaseTransaction(transactionId, new HashSet<HBaseCellId>(), tm);

        }

    }

    public static class Builder {
        Configuration conf = new Configuration();
        TSOClient tsoClient;
        CommitTable.Client commitTableClient;

        private Builder() {
        }

        public Builder withConfiguration(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public Builder withTSOClient(TSOClient tsoClient) {
            this.tsoClient = tsoClient;
            return this;
        }

        public Builder withCommitTableClient(CommitTable.Client client) {
            this.commitTableClient = client;
            return this;
        }

        public HBaseTransactionManager build() throws InstantiationException {
            boolean ownsTsoClient = false;
            if (tsoClient == null) {
                tsoClient = TSOClient.newBuilder()
                        .withConfiguration(convertToCommonsConf(conf))
                        .build();
                ownsTsoClient = true;
            }

            boolean ownsCommitTableClient = false;
            if (commitTableClient == null) {
                try {
                    HBaseCommitTableConfig config = new HBaseCommitTableConfig();
                    CommitTable commitTable =
                            new HBaseCommitTable(conf, config);
                    commitTableClient = commitTable.getClient().get();
                    ownsCommitTableClient = true;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InstantiationException("Interrupted whilst creating the HBase transaction manager");
                } catch (ExecutionException e) {
                    throw new InstantiationException("Exception whilst getting the CommitTable client");
                }
            }
            return new HBaseTransactionManager(tsoClient, ownsTsoClient,
                    commitTableClient, ownsCommitTableClient,
                    new HBaseTransactionFactory());
        }

        private org.apache.commons.configuration.Configuration convertToCommonsConf(Configuration hconf) {
            org.apache.commons.configuration.Configuration conf =
                    new org.apache.commons.configuration.BaseConfiguration();
            for (Map.Entry<String, String> e : hconf) {
                conf.addProperty(e.getKey(), e.getValue());
            }
            return conf;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private HBaseTransactionManager(TSOClient tsoClient,
                                    boolean ownsTSOClient,
                                    CommitTable.Client commitTableClient,
                                    boolean ownsCommitTableClient,
                                    HBaseTransactionFactory hBaseTransactionFactory) {
        super(tsoClient, ownsTSOClient, commitTableClient, ownsCommitTableClient, hBaseTransactionFactory);
    }

    @Override
    public void updateShadowCells(AbstractTransaction<? extends CellId> tx)
            throws TransactionManagerException {

        HBaseTransaction transaction = enforceHBaseTransactionAsParam(tx);

        Set<HBaseCellId> cells = (Set<HBaseCellId>) transaction.getWriteSet();

        // Add shadow cells
        for (HBaseCellId cell : cells) {
            Put put = new Put(cell.getRow());
            put.add(cell.getFamily(), addShadowCellSuffix(cell.getQualifier()), transaction.getStartTimestamp(),
                    Bytes.toBytes(transaction.getCommitTimestamp()));
            try {
                cell.getTable().put(put);
            } catch (IOException e) {
                LOG.warn("Failed inserting shadow cell {} for Tx {}", new Object[]{cell, transaction, e});
            }
        }
    }

    @Override
    public void preCommit(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {
        // Flush all pending writes
        flushTables(enforceHBaseTransactionAsParam(transaction));
    }

    @Override
    public void preRollback(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {
        // Flush all pending writes
        flushTables(enforceHBaseTransactionAsParam(transaction));
    }

    @Override
    public boolean isCommitted(HBaseCellId hBaseCellId) throws TransactionException {
        try {
            CommitTimestamp tentativeCommitTimestamp =
                    locateCellCommitTimestamp(hBaseCellId.getTimestamp(),
                            new CommitTimestampLocatorImpl(hBaseCellId, Maps.<Long, Long>newHashMap()));

            switch (tentativeCommitTimestamp.getLocation()) {
                case COMMIT_TABLE:
                case SHADOW_CELL:
                    return true;
                case NOT_PRESENT:
                    return false;
                case CACHE: // cache was empty
                default:
                    assert (false);
                    return false;
            }
        } catch (IOException e) {
            throw new TransactionException("Failure while checking if a transaction was committed", e);
        }
    }

    @Override
    public long getLowWatermark() throws TransactionException {
        try {
            return commitTableClient.readLowWatermark().get();
        } catch (ExecutionException ee) {
            throw new TransactionException("Error reading low watermark", ee.getCause());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException("Interrupted reading low watermark", ie);
        }
    }

    /**
     * Utility method that allows to add the shadow cell suffix to an HBase column qualifier.
     *
     * @param qualifier the qualifier to add the suffix to.
     * @return the suffixed qualifier
     */
    public static byte[] addShadowCellSuffix(byte[] qualifier) {
        return com.google.common.primitives.Bytes.concat(qualifier, SHADOW_CELL_SUFFIX);
    }

    /**
     * Utility method that allows know if a qualifier is a shadow cell column qualifier.
     *
     * @param qualifier the qualifier to learn whether is a shadow cell or not.
     * @return whether the qualifier passed is a shadow cell or not
     */
    public static boolean isShadowCell(byte[] qualifier) {
        int index = com.google.common.primitives.Bytes.indexOf(qualifier, SHADOW_CELL_SUFFIX);
        return index >= 0 && index == (qualifier.length - SHADOW_CELL_SUFFIX.length);
    }

    // ****************************************************************************************************************
    // Helper methods
    // ****************************************************************************************************************

    /**
     * Flushes pending operations for tables touched by transaction
     */
    private void flushTables(HBaseTransaction transaction) throws TransactionManagerException {
        try {
            for (HTableInterface writtenTable : transaction.getWrittenTables()) {
                writtenTable.flushCommits();
            }
        } catch (IOException e) {
            transaction.cleanup();
            throw new TransactionManagerException("Exception while flushing writes", e);
        }
    }

    private HBaseTransaction
    enforceHBaseTransactionAsParam(AbstractTransaction<? extends CellId> tx) {

        if (tx instanceof HBaseTransaction) {
            return (HBaseTransaction) tx;
        } else {
            throw new IllegalArgumentException(
                    "The transaction object passed is not an instance of HBaseTransaction");
        }

    }

    static class CommitTimestampLocatorImpl implements CommitTimestampLocator {
        private HBaseCellId hBaseCellId;
        private final Map<Long, Long> commitCache;

        public CommitTimestampLocatorImpl(HBaseCellId hBaseCellId, Map<Long, Long> commitCache) {
            this.hBaseCellId = hBaseCellId;
            this.commitCache = commitCache;
        }

        @Override
        public Optional<Long> readCommitTimestampFromCache(long startTimestamp) {
            if (commitCache.containsKey(startTimestamp)) {
                return Optional.of(commitCache.get(startTimestamp));
            }
            return Optional.absent();
        }

        @Override
        public Optional<Long> readCommitTimestampFromShadowCell(long startTimestamp)
                throws IOException {

            Get get = new Get(hBaseCellId.getRow());
            byte[] family = hBaseCellId.getFamily();
            byte[] shadowCellQualifier = HBaseTransactionManager.addShadowCellSuffix(hBaseCellId.getQualifier());
            get.addColumn(family, shadowCellQualifier);
            get.setMaxVersions(1);
            get.setTimeStamp(startTimestamp);
            Result result = hBaseCellId.getTable().get(get);
            if (result.containsColumn(family, shadowCellQualifier)) {
                return Optional.of(Bytes.toLong(result.getValue(family, shadowCellQualifier)));
            }
            return Optional.absent();
        }

    }

}
