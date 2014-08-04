package com.yahoo.omid.transaction;

import static com.yahoo.omid.committable.hbase.HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.tso.hbase.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
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

/**
 * Garbage collector for stale data: triggered upon HBase
 * compactions, it removes data from uncommitted transactions
 * older than the low watermark using a special scanner
 */
public class OmidCompactor extends BaseRegionObserver {

    private static final Logger LOG = LoggerFactory.getLogger(OmidCompactor.class);
    final static String OMID_COMPACTABLE_CF_FLAG = "OMID_ENABLED";

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
        TableName tableName = e.getEnvironment().getRegion().getTableDesc().getTableName();
        if (!tableName.isSystemTable()
                && !tableName.equals(TableName.valueOf(COMMIT_TABLE_DEFAULT_NAME))
                && !tableName.equals(TableName.valueOf(TIMESTAMP_TABLE_DEFAULT_NAME))) {
            if (commitTableClient == null) {
                LOG.trace("Initializing CommitTable client in preOpen for table {}", tableName);
                commitTableClient = initAndGetCommitTableClient();
            }
        }
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
                                      Store store,
                                      InternalScanner scanner,
                                      ScanType scanType) throws IOException
    {
        HTableDescriptor desc = e.getEnvironment().getRegion().getTableDesc();
        HColumnDescriptor famDesc
            = desc.getFamily(Bytes.toBytes(store.getColumnFamilyName()));
        boolean omidCompactable = Boolean.valueOf(famDesc.getValue(OMID_COMPACTABLE_CF_FLAG));
        // only column families tagged as compactable are compacted
        // with omid compactor
        if (!omidCompactable) {
            return scanner;
        } else {
            if (commitTableClient != null) {
                return new CompactorScanner(e, scanner, commitTableClient);
            }
            throw new IOException("CommitTable client was not be obtained when opening region for table " + desc.getTableName());
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

        private final HRegion hRegion;

        private boolean hasMoreRows = false;
        private List<Cell> currentRowWorthValues = new ArrayList<Cell>();

        public CompactorScanner(ObserverContext<RegionCoprocessorEnvironment> e,
                                InternalScanner internalScanner,
                                Client commitTableClient) throws IOException
        {
            this.internalScanner = internalScanner;
            this.commitTableClient = commitTableClient;
            this.lowWatermark = getLowWatermarkFromCommitTable();
            // Obtain the table in which the scanner is going to operate
            this.hRegion = e.getEnvironment().getRegion();
            LOG.info("Scanner cleaning up uncommitted txs older than LW [{}] in region [{}]",
                    lowWatermark, hRegion.getRegionInfo());
        }

        @Override
        public boolean next(List<Cell> results) throws IOException {
            return next(results, -1);
        }

        @Override
        public boolean next(List<Cell> result, int limit) throws IOException {

            if (currentRowWorthValues.isEmpty()) {
                // 1) Read next row
                List<Cell> scanResult = new ArrayList<Cell>();
                hasMoreRows = internalScanner.next(scanResult);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Row: Result {} limit {} more rows? {}",
                            new Object[] { scanResult, limit, hasMoreRows });
                }
                // 2) Traverse result list separating normal cells from shadow
                // cells and building a map to access easily the shadow cells.
                Map<Cell, Optional<Cell>> cellToSc = mapCellsToShadowCells(scanResult);

                // 3) traverse the list of row key values isolated before and
                // check which ones should be discarded
                for (Map.Entry<Cell, Optional<Cell>> entry : cellToSc.entrySet()) {
                    Cell cell = entry.getKey();
                    Optional<Cell> shadowCellOp = entry.getValue();

                    if ((cell.getTimestamp() > lowWatermark) || shadowCellOp.isPresent()) {
                        LOG.trace("Retaining cell {}", cell);
                        currentRowWorthValues.add(cell);
                        if (shadowCellOp.isPresent()) {
                            LOG.trace("...with shadow cell {}", cell, shadowCellOp.get());
                            currentRowWorthValues.add(shadowCellOp.get());
                        } else {
                            LOG.trace("...without shadow cell! (TS is above Low Watermark)");
                        }
                    } else {
                        Optional<Long> commitTimestamp = findMatchingCommitTimestampInCommitTable(cell.getTimestamp());
                        if (commitTimestamp.isPresent()) {
                            // Build the missing shadow cell...
                            byte[] shadowCellQualifier =
                                    HBaseUtils.addShadowCellSuffix(CellUtil.cloneQualifier(cell));
                            KeyValue shadowCell = new KeyValue(CellUtil.cloneRow(cell),
                                                               CellUtil.cloneFamily(cell),
                                                               shadowCellQualifier,
                                                               cell.getTimestamp(),
                                                               Bytes.toBytes(commitTimestamp.get()));
                            LOG.trace("Retaining cell {} with new shadow cell {}", cell, shadowCell);
                            currentRowWorthValues.add(cell);
                            currentRowWorthValues.add(shadowCell);
                        } else {
                            // The shadow cell could have been added in the time period
                            // after the scanner had been created, so we check it
                            if (HBaseUtils.hasShadowCell(cell, new HRegionCellGetterAdapter(hRegion))) {
                                LOG.trace("Retaining cell {} without its recently found shadow cell", cell);
                                currentRowWorthValues.add(cell);
                            } else {
                                LOG.trace("Discarding cell {}", cell);
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

        private static Map<Cell, Optional<Cell>> mapCellsToShadowCells(List<Cell> result)
                throws IOException {

            Map<Cell, Optional<Cell>> cellToShadowCellMap = new HashMap<Cell, Optional<Cell>>();

            Map<CellId, Cell> cellIdToCellMap = new HashMap<CellId, Cell>();
            for (Cell cell : result) {
                if (!HBaseUtils.isShadowCell(CellUtil.cloneQualifier(cell))) {
                    CellId key = new CellId(CellUtil.cloneRow(cell),
                                            CellUtil.cloneFamily(cell),
                                            CellUtil.cloneQualifier(cell),
                                            cell.getTimestamp());
                    if (cellIdToCellMap.containsKey(key)) {
                        throw new IOException(
                                "A value is already present for key " + key + ". This should not happen");
                    }
                    LOG.trace("Adding KV key {} to map with absent value", cell);
                    cellToShadowCellMap.put(cell, Optional.<Cell> absent());
                    cellIdToCellMap.put(key, cell);
                } else {
                    byte[] originalQualifier = HBaseUtils.removeShadowCellSuffix(CellUtil.cloneQualifier(cell));
                    CellId key = new CellId(CellUtil.cloneRow(cell),
                                            CellUtil.cloneFamily(cell),
                                            originalQualifier,
                                            cell.getTimestamp());
                    if (cellIdToCellMap.containsKey(key)) {
                        Cell originalCell = cellIdToCellMap.get(key);
                        LOG.trace("Adding to key {} value {}", key, cell);
                        cellToShadowCellMap.put(originalCell, Optional.of(cell));
                    } else {
                        LOG.trace("Map does not contain key {}", key);
                    }
                }
            }

            return cellToShadowCellMap;
        }

        private static class CellId {

            private static final int MIN_BITS = 32;

            private final byte[] row;
            private final byte[] family;
            private final byte[] qualifier;
            private final long timestamp;

            public CellId(
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
                if (!(o instanceof CellId))
                    return false;
                CellId otherCellId = (CellId) o;
                return Arrays.equals(otherCellId.getRow(), row)
                        && Arrays.equals(otherCellId.getFamily(), family)
                        && Arrays.equals(otherCellId.getQualifier(), qualifier)
                        && otherCellId.getTimestamp() == timestamp;
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
