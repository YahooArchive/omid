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

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.CommitTimestamp;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTableConfig;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import com.yahoo.omid.tsoclient.CellId;
import com.yahoo.omid.tsoclient.TSOClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.yahoo.omid.committable.hbase.CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.committable.hbase.CommitTableConstants.COMMIT_TABLE_NAME_KEY;

public class HBaseTransactionManager extends AbstractTransactionManager implements HBaseTransactionClient {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseTransactionManager.class);

    static final byte[] SHADOW_CELL_SUFFIX = "\u0080".getBytes(Charsets.UTF_8); // Non printable char (128 ASCII)

    public enum PostCommitMode {
        SYNC, ASYNC
    }

    private static class HBaseTransactionFactory implements TransactionFactory<HBaseCellId> {

        @Override
        public HBaseTransaction createTransaction(long transactionId, long epoch, AbstractTransactionManager tm) {

            return new HBaseTransaction(transactionId, epoch, new HashSet<HBaseCellId>(), tm);

        }

    }

    public static class Builder {
        Configuration conf = new Configuration();
        MetricsRegistry metricsRegistry = new NullMetricsProvider();
        PostCommitMode postCommitMode = PostCommitMode.SYNC;
        TSOClient tsoClient;
        CommitTable.Client commitTableClient;
        PostCommitActions postCommitter;

        private Builder() {
        }

        public Builder withConfiguration(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public Builder withMetrics(MetricsRegistry metricsRegistry) {
            this.metricsRegistry = metricsRegistry;
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

        public Builder postCommitter(PostCommitActions postCommitter) {
            this.postCommitter = postCommitter;
            return this;
        }

        public Builder postCommitMode(PostCommitMode postCommitMode) {
            this.postCommitMode = postCommitMode;
            return this;
        }

        public HBaseTransactionManager build() throws OmidInstantiationException {

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
                    String commitTableName = conf.get(COMMIT_TABLE_NAME_KEY, COMMIT_TABLE_DEFAULT_NAME);
                    HBaseCommitTableConfig config = new HBaseCommitTableConfig(commitTableName);
                    CommitTable commitTable = new HBaseCommitTable(conf, config);
                    commitTableClient = commitTable.getClient();
                    ownsCommitTableClient = true;
                } catch (IOException e) {
                    throw new OmidInstantiationException("Exception whilst getting the CommitTable client", e);
                }
            }

            if (postCommitter == null) {
                PostCommitActions syncPostCommitter = new HBaseSyncPostCommitter(metricsRegistry, commitTableClient);
                switch(postCommitMode) {
                    case ASYNC:
                        postCommitter = new HBaseAsyncPostCommitter(syncPostCommitter);
                        break;
                    case SYNC:
                    default:
                        postCommitter = syncPostCommitter;
                        break;
                }
            }

            return new HBaseTransactionManager(metricsRegistry,
                                               postCommitter,
                                               tsoClient,
                                               ownsTsoClient,
                                               commitTableClient,
                                               ownsCommitTableClient,
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

    private HBaseTransactionManager(MetricsRegistry metrics,
                                    PostCommitActions postCommitter,
                                    TSOClient tsoClient,
                                    boolean ownsTSOClient,
                                    CommitTable.Client commitTableClient,
                                    boolean ownsCommitTableClient,
                                    HBaseTransactionFactory hBaseTransactionFactory)
    {

        super(metrics,
              postCommitter,
              tsoClient,
              ownsTSOClient,
              commitTableClient,
              ownsCommitTableClient,
              hBaseTransactionFactory);

    }

    @Override
    public void preCommit(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {
        try {
            // Flush all pending writes
            HBaseTransaction hBaseTx = enforceHBaseTransactionAsParam(transaction);
            hBaseTx.flushTables();
        } catch (IOException e) {
            throw new TransactionManagerException("Exception while flushing writes", e);
        }
    }

    @Override
    public void preRollback(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException {
        try {
            // Flush all pending writes
            HBaseTransaction hBaseTx = enforceHBaseTransactionAsParam(transaction);
            hBaseTx.flushTables();
        } catch (IOException e) {
            throw new TransactionManagerException("Exception while flushing writes", e);
        }
    }

    @Override
    public boolean isCommitted(HBaseCellId hBaseCellId) throws TransactionException {
        try {
            CommitTimestamp tentativeCommitTimestamp =
                    locateCellCommitTimestamp(hBaseCellId.getTimestamp(), tsoClient.getEpoch(),
                                              new CommitTimestampLocatorImpl(hBaseCellId, Maps.<Long, Long>newHashMap()));

            // If transaction that added the cell was invalidated
            if (!tentativeCommitTimestamp.isValid()) {
                return false;
            }

            switch (tentativeCommitTimestamp.getLocation()) {
                case COMMIT_TABLE:
                case SHADOW_CELL:
                    return true;
                case NOT_PRESENT:
                    return false;
                case CACHE: // cache was empty
                default:
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

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    // TODO: move to hbase commons package
    static HBaseTransaction enforceHBaseTransactionAsParam(AbstractTransaction<? extends CellId> tx) {

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
            byte[] shadowCellQualifier = CellUtils.addShadowCellSuffix(hBaseCellId.getQualifier());
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
