package com.yahoo.omid.transaction;

import static com.yahoo.omid.committable.hbase.HBaseCommitTable.HBASE_COMMIT_TABLE_NAME_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
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

    private HBaseCommitTableConfig commitTableConf = null;
    private Configuration conf = null;
    @VisibleForTesting
    protected Queue<CommitTable.Client> commitTableClientQueue = new ConcurrentLinkedQueue<>();

    public OmidCompactor() {
        LOG.info("Compactor coprocessor initialized via empty constructor");
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        LOG.info("Starting compactor coprocessor");
        this.conf = env.getConfiguration();
        this.commitTableConf = new HBaseCommitTableConfig();
        String commitTableName = this.conf.get(HBASE_COMMIT_TABLE_NAME_KEY);
        if (commitTableName != null) {
            this.commitTableConf.setTableName(commitTableName);
        }
        LOG.info("Compactor coprocessor started");
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        LOG.info("Stopping compactor coprocessor");
        if (commitTableClientQueue != null) {
            for (CommitTable.Client commitTableClient : commitTableClientQueue) {
                commitTableClient.close();
            }
        }
        LOG.info("Compactor coprocessor stopped");
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
            CommitTable.Client commitTableClient = commitTableClientQueue.poll();
            if (commitTableClient == null) {
                commitTableClient = initAndGetCommitTableClient();
            }
            return new CompactorScanner(e, scanner, commitTableClient, commitTableClientQueue);
        }
    }

    private CommitTable.Client initAndGetCommitTableClient() throws IOException {
        LOG.info("Trying to get the commit table client");
        CommitTable commitTable = new HBaseCommitTable(conf, commitTableConf);
        try {
            CommitTable.Client commitTableClient = commitTable.getClient().get();
            LOG.info("Commit table client obtained {}", commitTableClient.getClass().getCanonicalName());
            return commitTableClient;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted getting the commit table client");
        } catch (ExecutionException ee) {
            throw new IOException("Error getting the commit table client", ee.getCause());
        }
    }

    static class CompactorScanner implements InternalScanner {
        private final InternalScanner internalScanner;
        private final CommitTable.Client commitTableClient;
        private final Queue<CommitTable.Client> commitTableClientQueue;
        private final long lowWatermark;

        private final HRegion hRegion;

        private boolean hasMoreRows = false;
        private List<Cell> currentRowWorthValues = new ArrayList<Cell>();

        public CompactorScanner(ObserverContext<RegionCoprocessorEnvironment> e,
                                InternalScanner internalScanner,
                                Client commitTableClient,
                                Queue<CommitTable.Client> commitTableClientQueue) throws IOException
        {
            this.internalScanner = internalScanner;
            this.commitTableClient = commitTableClient;
            this.commitTableClientQueue = commitTableClientQueue;
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

        private static void retain(List<Cell> result,
                                   Cell cell, Optional<Cell> shadowCell) {
            LOG.trace("Retaining cell {}", cell);
            result.add(cell);
            if (shadowCell.isPresent()) {
                LOG.trace("...with shadow cell {}", cell, shadowCell.get());
                result.add(shadowCell.get());
            } else {
                LOG.trace("...without shadow cell! (TS is above Low Watermark)");
            }
        }

        private static void skipToNextColumn(Cell cell,
                                             PeekingIterator<Map.Entry<Cell, Optional<Cell>>> iter) {
            while (iter.hasNext()
                   && CellUtil.matchingFamily(iter.peek().getKey(), cell)
                   && CellUtil.matchingQualifier(iter.peek().getKey(), cell)) {
                iter.next();
            }
        }

        private static boolean isTombstone(Cell cell) {
            return CellUtil.matchingValue(cell, TTable.DELETE_TOMBSTONE);
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
                SortedMap<Cell, Optional<Cell>> cellToSc = mapCellsToShadowCells(scanResult);

                // 3) traverse the list of row key values isolated before and
                // check which ones should be discarded
                PeekingIterator<Map.Entry<Cell, Optional<Cell>>> iter
                    = Iterators.peekingIterator(cellToSc.entrySet().iterator());
                while (iter.hasNext()) {
                    Map.Entry<Cell, Optional<Cell>> entry = iter.next();
                    Cell cell = entry.getKey();
                    Optional<Cell> shadowCellOp = entry.getValue();

                    if (cell.getTimestamp() > lowWatermark) {
                        retain(currentRowWorthValues, cell, shadowCellOp);
                    } else if (isTombstone(cell)) {
                        if (shadowCellOp.isPresent()) {
                            skipToNextColumn(cell, iter);
                        } else {
                            Optional<Long> commitTimestamp = queryCommitTimestamp(cell);
                            if (commitTimestamp.isPresent()) {
                                skipToNextColumn(cell, iter);
                            }
                        }
                    } else if (shadowCellOp.isPresent()) {
                        retain(currentRowWorthValues, cell, shadowCellOp);
                    } else {
                        Optional<Long> commitTimestamp = queryCommitTimestamp(cell);
                        if (commitTimestamp.isPresent()) {
                            // Build the missing shadow cell...
                            byte[] shadowCellQualifier =
                                    HBaseUtils.addShadowCellSuffix(CellUtil.cloneQualifier(cell));
                            KeyValue shadowCell = new KeyValue(CellUtil.cloneRow(cell),
                                                               CellUtil.cloneFamily(cell),
                                                               shadowCellQualifier,
                                                               cell.getTimestamp(),
                                                               Bytes.toBytes(commitTimestamp.get()));
                            LOG.trace("Retaining cell {} with shadow cell {}", cell, shadowCell);
                            currentRowWorthValues.add(cell);
                            currentRowWorthValues.add(shadowCell);
                        } else {
                            LOG.trace("Discarding cell {}", cell);
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
            commitTableClientQueue.add(commitTableClient);
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

        private Optional<Long> queryCommitTimestamp(Cell cell) throws IOException {
            Optional<Long> commitTimestamp = queryCommitTable(cell.getTimestamp());
            if (commitTimestamp.isPresent()) {
                return commitTimestamp;
            } else {
                Get g = new Get(CellUtil.cloneRow(cell));
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = HBaseUtils.addShadowCellSuffix(CellUtil.cloneQualifier(cell));
                g.addColumn(family, qualifier);
                g.setTimeStamp(cell.getTimestamp());
                Result r = hRegion.get(g);
                if (r.containsColumn(family, qualifier)) {
                    return Optional.of(Bytes.toLong(r.getValue(family, qualifier)));
                }
            }
            return Optional.absent();
        }

        private Optional<Long> queryCommitTable(long startTimestamp) throws IOException {
            try {
                return commitTableClient.getCommitTimestamp(startTimestamp).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while getting commit timestamp from commit table");
            } catch (ExecutionException e) {
                throw new IOException("Error getting commit timestamp from commit table", e);
            }

        }

        private static SortedMap<Cell, Optional<Cell>> mapCellsToShadowCells(List<Cell> result)
                throws IOException {

            SortedMap<Cell, Optional<Cell>> cellToShadowCellMap
                = new TreeMap<Cell, Optional<Cell>>(new CellComparator());

            Map<CellId, Cell> cellIdToCellMap = new HashMap<CellId, Cell>();
            for (Cell cell : result) {
                if (!HBaseUtils.isShadowCell(CellUtil.cloneQualifier(cell))) {
                    CellId key = new CellId(CellUtil.cloneRow(cell),
                                            CellUtil.cloneFamily(cell),
                                            CellUtil.cloneQualifier(cell),
                                            cell.getTimestamp());
                    if (cellIdToCellMap.containsKey(key)
                        && !cellIdToCellMap.get(key).equals(cell)) {
                        throw new IOException(
                                "A value is already present for key " + key +
                                ". This should not happen. Current row elements: " + result);
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

        static class CellId {

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

            @Override
            public String toString() {
                return Objects.toStringHelper(this)
                              .add("row", Bytes.toStringBinary(row))
                              .add("family", Bytes.toString(family))
                              .add("qualifier", Bytes.toString(qualifier))
                              .add("ts", timestamp)
                              .toString();
            }
        }

    }

}
