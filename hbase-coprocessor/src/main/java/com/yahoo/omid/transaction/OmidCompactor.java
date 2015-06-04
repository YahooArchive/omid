package com.yahoo.omid.transaction;

import static com.yahoo.omid.committable.CommitTable.CommitTimestamp.Location.SHADOW_CELL;
import static com.yahoo.omid.committable.hbase.HBaseCommitTable.HBASE_COMMIT_TABLE_NAME_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.Client;
import com.yahoo.omid.committable.CommitTable.CommitTimestamp;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTableConfig;
import com.yahoo.omid.transaction.CellUtils.CellInfo;

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
                                      ScanType scanType,
                                      CompactionRequest request) throws IOException
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
            boolean isMajorCompaction = request.isMajor();
            return new CompactorScanner(e, scanner, commitTableClient, commitTableClientQueue, isMajorCompaction);
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
        private final boolean isMajorCompaction;
        private final long lowWatermark;

        private final HRegion hRegion;

        private boolean hasMoreRows = false;
        private List<Cell> currentRowWorthValues = new ArrayList<Cell>();

        public CompactorScanner(ObserverContext<RegionCoprocessorEnvironment> e,
                                InternalScanner internalScanner,
                                Client commitTableClient,
                                Queue<CommitTable.Client> commitTableClientQueue,
                                boolean isMajorCompaction) throws IOException
        {
            this.internalScanner = internalScanner;
            this.commitTableClient = commitTableClient;
            this.commitTableClientQueue = commitTableClientQueue;
            this.isMajorCompaction = isMajorCompaction;
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
                SortedMap<Cell, Optional<Cell>> cellToSc = CellUtils.mapCellsToShadowCells(scanResult);

                // 3) traverse the list of row key values isolated before and
                // check which ones should be discarded
                Map<String, CellInfo> lastTimestampedCellsInRow = new HashMap<>();
                PeekingIterator<Map.Entry<Cell, Optional<Cell>>> iter
                    = Iterators.peekingIterator(cellToSc.entrySet().iterator());
                while (iter.hasNext()) {
                    Map.Entry<Cell, Optional<Cell>> entry = iter.next();
                    Cell cell = entry.getKey();
                    Optional<Cell> shadowCellOp = entry.getValue();

                    if (cell.getTimestamp() > lowWatermark) {
                        retain(currentRowWorthValues, cell, shadowCellOp);
                        continue;
                    }

                    // During a minor compaction the coprocessor may only see a
                    // subset of store files and may not have the all the versions
                    // of a cell available for consideration. Therefore, if it
                    // deletes a cell with a tombstone during a minor compaction,
                    // an older version of the cell may become visible again. So,
                    // we have to remove tombstones only in major compactions.
                    if (isMajorCompaction) {
                        if (CellUtils.isTombstone(cell)) {
                            if (shadowCellOp.isPresent()) {
                                skipToNextColumn(cell, iter);
                            } else {
                                Optional<CommitTimestamp> commitTimestamp = queryCommitTimestamp(cell);
                                // Clean the cell only if it is valid
                                if (commitTimestamp.isPresent() && commitTimestamp.get().isValid()) {
                                    skipToNextColumn(cell, iter);
                                }
                            }
                            continue;
                        }
                    } else {
                        // In minor compactions, if we found a cell which has
                        // been marked by HBase as Delete or Delete Family
                        // (that is, non-transactionally deleted), we retain
                        // it just in case
                        if (CellUtil.isDelete(cell)
                                ||
                            CellUtil.isDeleteFamily(cell)) {
                            retain(currentRowWorthValues, cell, shadowCellOp);
                        }

                    }

                    if (shadowCellOp.isPresent()) {
                        saveLastTimestampedCell(lastTimestampedCellsInRow, cell, shadowCellOp.get());
                    } else {
                        Optional<CommitTimestamp> commitTimestamp = queryCommitTimestamp(cell);
                        if (commitTimestamp.isPresent() && commitTimestamp.get().isValid()) {
                            // Build the missing shadow cell...
                            byte[] shadowCellValue = Bytes.toBytes(commitTimestamp.get().getValue());
                            Cell shadowCell = CellUtils.buildShadowCellFromCell(cell, shadowCellValue);
                            saveLastTimestampedCell(lastTimestampedCellsInRow, cell, shadowCell);
                        } else {
                            LOG.trace("Discarding cell {}", cell);
                        }
                    }
                }
                retainLastTimestampedCellsSaved(currentRowWorthValues, lastTimestampedCellsInRow);

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

        // ********************************************************************
        // *
        // * Private methods
        // *
        // ********************************************************************

        private void saveLastTimestampedCell(Map<String, CellInfo> lastCells, Cell cell, Cell shadowCell) {
            String cellKey = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
                    + ":"
                    + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            LOG.trace("Cell Key: {}", cellKey);

            if (!lastCells.containsKey(cellKey)) {
                lastCells.put(cellKey, new CellInfo(cell, shadowCell));
            } else {
                if (lastCells.get(cellKey).getTimestamp() < cell.getTimestamp()) {
                    lastCells.put(cellKey, new CellInfo(cell, shadowCell));
                } else {
                    LOG.trace("Forgetting old cell {}", cell);
                }
            }
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

        private Optional<CommitTimestamp> queryCommitTimestamp(Cell cell) throws IOException {
            try {
                Optional<CommitTimestamp> ct = commitTableClient.getCommitTimestamp(cell.getTimestamp()).get();
                if (ct.isPresent()) {
                    return Optional.of(ct.get());
                } else {
                    Get g = new Get(CellUtil.cloneRow(cell));
                    byte[] family = CellUtil.cloneFamily(cell);
                    byte[] qualifier = CellUtils.addShadowCellSuffix(cell.getQualifierArray(),
                            cell.getQualifierOffset(),
                            cell.getQualifierLength());
                    g.addColumn(family, qualifier);
                    g.setTimeStamp(cell.getTimestamp());
                    Result r = hRegion.get(g);
                    if (r.containsColumn(family, qualifier)) {
                        return Optional.of(new CommitTimestamp(SHADOW_CELL,
                                Bytes.toLong(r.getValue(family, qualifier)), true));
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while getting commit timestamp from commit table");
            } catch (ExecutionException e) {
                throw new IOException("Error getting commit timestamp from commit table", e);
            }

            return Optional.absent();
        }

        private void retain(List<Cell> result,
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

        private void retainLastTimestampedCellsSaved(
                            List<Cell> result,
                            Map<String, CellInfo> lastTimestampedCellsInRow) {

            for (CellInfo cellInfo : lastTimestampedCellsInRow.values()) {
                LOG.trace("Retaining last cell {} with shadow cell {}",
                        cellInfo.getCell(),
                        cellInfo.getShadowCell());
                result.add(cellInfo.getCell());
                result.add(cellInfo.getShadowCell());
            }
        }

        private void skipToNextColumn(Cell cell,
                                             PeekingIterator<Map.Entry<Cell, Optional<Cell>>> iter) {
            while (iter.hasNext()
                   && CellUtil.matchingFamily(iter.peek().getKey(), cell)
                   && CellUtil.matchingQualifier(iter.peek().getKey(), cell)) {
                iter.next();
            }
        }

    }

}
