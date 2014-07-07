package com.yahoo.omid.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.hash.Hashing;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.Client;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTableConfig;
import com.yahoo.omid.tso.hbase.HBaseTimestampStorage;

/**
 * Garbage collector for stale data: triggered upon HBase
 * compactions, it removes data from uncommitted transactions
 * older than the low watermark using a special scanner
 */
public class OmidCompactor extends BaseRegionObserver {

    private static final Logger LOG = LoggerFactory.getLogger(OmidCompactor.class);

    private Configuration conf = null;
    CommitTable.Client commitTableClient = null;

    public OmidCompactor() {
        LOG.info("Compactor coprocessor initialized via empty constructor");
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        LOG.info("Starting compactor coprocessor");
        this.conf = env.getConfiguration();
        LOG.info("Compactor coprocessor started");
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        LOG.info("Stopping compactor coprocessor");
        if (commitTableClient != null) {
            commitTableClient.close();
        }
        LOG.info("Compactor coprocessor stopped");
    }

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        // Initialize the commit table accessor the first time. Note
        // this can't be done in the start() method above because the
        // HBase server need to be initialized first
        String tableName = Bytes.toString(e.getEnvironment().getRegion().getTableDesc().getName());
        if (!e.getEnvironment().getRegion().getRegionInfo().isMetaTable()
                && !tableName.equals(HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME)
                && !tableName.equals(HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME)) {
            if (commitTableClient == null) {
                LOG.trace("Initializing CommitTable client in preOpen for table {}", tableName);
                commitTableClient = initAndGetCommitTableClient();
            }
        }
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
                                      Store store,
                                      InternalScanner scanner) throws IOException
    {
        String tableName = Bytes.toString(e.getEnvironment().getRegion().getTableDesc().getName());
        if (e.getEnvironment().getRegion().getRegionInfo().isMetaTable()
                || tableName.equals(HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME)
                || tableName.equals(HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME)) {
            return scanner;
        } else {
            if (commitTableClient != null) {
                return new CompactorScanner(e, scanner, commitTableClient);
            }
            throw new IOException("CommitTable client was not be obtained when opening region for table " + tableName);
        }
    }

    public CommitTable.Client initAndGetCommitTableClient() throws IOException {
        LOG.info("Trying to get the commit table client");
        CommitTable commitTable = new HBaseCommitTable(conf, new HBaseCommitTableConfig());
        try {
            commitTableClient = commitTable.getClient().get();
            LOG.info("Commit table client obtained {}", commitTableClient.getClass().getCanonicalName());
            return commitTableClient;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted getting the commit table client");
        } catch (ExecutionException ee) {
            throw new IOException("Error getting the commit table client", ee.getCause());
        }
    }

    private static class CompactorScanner implements InternalScanner {
        private final InternalScanner internalScanner;
        private final CommitTable.Client commitTableClient;
        private final long lowWatermark;

        private final HTableInterface table;

        private boolean hasMoreRows = false;
        private List<KeyValue> currentRowWorthValues = new ArrayList<KeyValue>();

        public CompactorScanner(ObserverContext<RegionCoprocessorEnvironment> e,
                                InternalScanner internalScanner,
                                Client commitTableClient) throws IOException
        {
            this.internalScanner = internalScanner;
            this.commitTableClient = commitTableClient;
            this.lowWatermark = getLowWatermarkFromCommitTable();
            LOG.trace("Low Watermark obtained " + lowWatermark);
            // Obtain the table in which the scanner is going to operate
            byte[] tableName = e.getEnvironment().getRegion().getTableDesc().getName();
            this.table = e.getEnvironment().getTable(tableName);
            LOG.info("Scanner cleaning up uncommitted txs older than LW [{}] in table [{}]",
                    lowWatermark,
                    Bytes.toString(tableName));
        }

        @Override
        public boolean next(List<KeyValue> results) throws IOException {
            return next(results, -1);
        }

        @Override
        public boolean next(List<KeyValue> results, String metric) throws IOException {
            return next(results);
        }

        @Override
        public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
            return next(result, limit);
        }

        @Override
        public boolean next(List<KeyValue> result, int limit) throws IOException {

            if (currentRowWorthValues.isEmpty()) {
                // 1) Read next row
                List<KeyValue> scanResult = new ArrayList<KeyValue>();
                hasMoreRows = internalScanner.next(scanResult);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Row: Result {} limit {} more rows? {}",
                            new Object[] { scanResult, limit, hasMoreRows });
                }
                // 2) Traverse result list separating normal key values from shadow
                // cells and building a map to access easily the shadow cells.
                Map<KeyValue, Optional<KeyValue>> kvToSc = mapKeyValuesToShadowCells(scanResult);

                // 3) traverse the list of row key values isolated before and
                // check which ones should be discarded
                for (Map.Entry<KeyValue, Optional<KeyValue>> entry : kvToSc.entrySet()) {
                    KeyValue kv = entry.getKey();
                    Optional<KeyValue> shadowCellKeyValueOp = entry.getValue();

                    if ((kv.getTimestamp() > lowWatermark) || shadowCellKeyValueOp.isPresent()) {
                        LOG.trace("Retaining cell {}", kv);
                        currentRowWorthValues.add(kv);
                        if (shadowCellKeyValueOp.isPresent()) {
                            LOG.trace("...with shadow cell {}", kv, shadowCellKeyValueOp.get());
                            currentRowWorthValues.add(shadowCellKeyValueOp.get());
                        } else {
                            LOG.trace("...without shadow cell! (TS is above Low Watermark)");
                        }
                    } else {
                        Optional<Long> commitTimestamp = findMatchingCommitTimestampInCommitTable(kv.getTimestamp());
                        if (commitTimestamp.isPresent()) {
                            // Build the missing shadow cell...
                            byte[] shadowCellQualifier =
                                    HBaseUtils.addShadowCellSuffix(kv.getQualifier());
                            KeyValue shadowCell = new KeyValue(kv.getRow(),
                                                               kv.getFamily(),
                                                               shadowCellQualifier,
                                                               kv.getTimestamp(),
                                                               Bytes.toBytes(commitTimestamp.get()));
                            LOG.trace("Retaining cell {} with new shadow cell {}", kv, shadowCell);
                            currentRowWorthValues.add(kv);
                            currentRowWorthValues.add(shadowCell);
                        } else {
                            // The shadow cell could have been added in the time period
                            // after the scanner had been created, so we check it
                            if (HBaseUtils.hasShadowCell(table, kv)) {
                                LOG.trace("Retaining cell {} without its recently found shadow cell", kv);
                                currentRowWorthValues.add(kv);
                            } else {
                                LOG.trace("Discarding cell {}", kv);
                            }
                        }
                    }
                }

                // 4) Sort the list
                Collections.sort(currentRowWorthValues, KeyValue.COMPARATOR);
            }

            // Chomp current row worth values up to the limit
            if(currentRowWorthValues.size() <= limit) {
                result.addAll(currentRowWorthValues);
                currentRowWorthValues.clear();
            } else {
                result.addAll(currentRowWorthValues.subList(0, limit));
                currentRowWorthValues.subList(0, limit).clear();
            }
            LOG.trace("Results to preserve {}", result);

            return hasMoreRows;
        }

        @Override
        public void close() throws IOException {
            internalScanner.close();
        }

        private long getLowWatermarkFromCommitTable() throws IOException {
            try {
                LOG.trace("About to read log watermark from commit table");
                return commitTableClient.readLowWatermark().get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted getting low watermark from commit table", ie);
                throw new IOException("Interrupted getting low watermark from commit table");
            } catch (ExecutionException ee) {
                LOG.warn("Problem getting low watermark from commit table");
                throw new IOException("Problem getting low watermark from commit table", ee.getCause());
            }
        }

        private Optional<Long> findMatchingCommitTimestampInCommitTable(long startTimestamp) throws IOException {

            try {
                return commitTableClient.getCommitTimestamp(startTimestamp).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while getting commit timestamp from commit table");
            } catch (ExecutionException e) {
                throw new IOException("Error getting commit timestamp from commit table", e);
            }

        }

        private static Map<KeyValue, Optional<KeyValue>> mapKeyValuesToShadowCells(List<KeyValue> result)
                throws IOException {

            Map<KeyValue, Optional<KeyValue>> kvToShadowCellMap = new HashMap<KeyValue, Optional<KeyValue>>();

            Map<KeyValueId, KeyValue> kvsTokv = new HashMap<KeyValueId, KeyValue>();
            for (KeyValue kv : result) {
                if (!HBaseUtils.isShadowCell(kv.getQualifier())) {
                    KeyValueId key = new KeyValueId(kv.getRow(),
                                                    kv.getFamily(),
                                                    kv.getQualifier(),
                                                    kv.getTimestamp());
                    if (kvsTokv.containsKey(key)) {
                        throw new IOException(
                                "A value is already present for key " + key + ". This should not happen");
                    }
                    LOG.trace("Adding KV key {} to map with absent value", kv);
                    kvToShadowCellMap.put(kv, Optional.<KeyValue> absent());
                    kvsTokv.put(key, kv);
                } else {
                    byte[] originalQualifier = HBaseUtils.removeShadowCellSuffix(kv.getQualifier());
                    KeyValueId key = new KeyValueId(kv.getRow(),
                                                    kv.getFamily(),
                                                    originalQualifier,
                                                    kv.getTimestamp());
                    if (kvsTokv.containsKey(key)) {
                        KeyValue originalKV = kvsTokv.get(key);
                        LOG.trace("Adding to key {} value {}", key, kv);
                        kvToShadowCellMap.put(originalKV, Optional.of(kv));
                    } else {
                        LOG.trace("Map does not contain key {}", key);
                    }
                }
            }

            return kvToShadowCellMap;
        }

        private static class KeyValueId {

            private static final int MIN_BITS = 32;

            private final byte[] row;
            private final byte[] family;
            private final byte[] qualifier;
            private final long timestamp;

            public KeyValueId(
                    byte[] row, byte[] family, byte[] qualifier, long timestamp) {

                this.row = row;
                this.family = family;
                this.qualifier = qualifier;
                this.timestamp = timestamp;
            }

            byte[] getRow() {
                return row;
            }

            byte[] getFamily() {
                return family;
            }

            byte[] getQualifier() {
                return qualifier;
            }

            long getTimestamp() {
                return timestamp;
            }

            @Override
            public boolean equals(Object o) {
                if (o == this)
                    return true;
                if (!(o instanceof KeyValueId))
                    return false;
                KeyValueId otherKVSkel = (KeyValueId) o;
                return Arrays.equals(otherKVSkel.getRow(), row)
                        && Arrays.equals(otherKVSkel.getFamily(), family)
                        && Arrays.equals(otherKVSkel.getQualifier(), qualifier)
                        && otherKVSkel.getTimestamp() == timestamp;
            }

            @Override
            public int hashCode() {
                return Hashing.goodFastHash(MIN_BITS).newHasher()
                        .putBytes(row)
                        .putBytes(family)
                        .putBytes(qualifier)
                        .putLong(timestamp)
                        .hash().asInt();
            }
        }

    }

}