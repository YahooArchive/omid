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

package com.yahoo.omid.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Provides transactional methods for accessing and modifying a given snapshot
 * of data identified by an opaque {@link Transaction} object.
 *
 */
public class TTable {
    private static Logger LOG = LoggerFactory.getLogger(TTable.class);

    public static long getsPerformed = 0;
    public static long elementsGotten = 0;
    public static long elementsRead = 0;
    public static long extraGetsPerformed = 0;
    public static double extraVersionsAvg = 3;

    public static byte[] DELETE_TOMBSTONE = Bytes.toBytes("__OMID_TOMBSTONE__");;

    /** We always ask for CACHE_VERSIONS_OVERHEAD extra versions */
    private static int CACHE_VERSIONS_OVERHEAD = 3;
    /** Average number of versions needed to reach the right snapshot */
    public double versionsAvg = 3;
    /** How fast do we adapt the average */
    private static final double ALPHA = 0.975;

    private final HTableInterface healerTable;

    private class ShadowCellsHealerTask implements Runnable {

        private final KeyValue kv;
        private final long commitTimestamp;

        public ShadowCellsHealerTask(KeyValue kv, long commitTimestamp) {
            this.kv = kv;
            this.commitTimestamp = commitTimestamp;
        }

        @Override
        public void run() {
            Put put = new Put(kv.getRow());
            byte[] family = kv.getFamily();
            byte[] shadowCellQualifier = TransactionManager.addShadowCellSuffix(kv.getQualifier());
            put.add(family, shadowCellQualifier, kv.getTimestamp(), Bytes.toBytes(commitTimestamp));
            try {
                healerTable.put(put);
            } catch (IOException e) {
                LOG.warn("Failed healing shadow cell for kv {}", kv, e);
            }
        }

    }

    ExecutorService shadowCellHealerExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("sc-healer-%d").build());

    private HTableInterface table;

    public TTable(Configuration conf, byte[] tableName) throws IOException {
        this(new HTable(conf, tableName));
    }

    public TTable(Configuration conf, String tableName) throws IOException {
        this(conf, Bytes.toBytes(tableName));
    }

    public TTable(HTableInterface hTable) throws IOException {
        table = hTable;
        healerTable = new HTable(table.getConfiguration(), table.getTableName());
    }

    /**
     * Extracts certain cells from a given row.
     *
     * @param get
     *            The object that specifies what data to fetch and from which
     *            row.
     * @return The data coming from the specified row, if it exists. If the row
     *         specified doesn't exist, the {@link Result} instance returned
     *         won't contain any {@link KeyValue}, as indicated by
     *         {@link Result#isEmpty()}.
     * @throws IOException
     *             if a remote or network exception occurs.
     */
    public Result get(Transaction transaction, final Get get) throws IOException {

        throwExceptionIfOpSetsTimerange(get);

        final int requestedVersions = (int) (versionsAvg + CACHE_VERSIONS_OVERHEAD);
        final long readTimestamp = transaction.getStartTimestamp();
        final Get tsget = new Get(get.getRow());
        TimeRange timeRange = get.getTimeRange();
        long startTime = timeRange.getMin();
        long endTime = Math.min(timeRange.getMax(), readTimestamp + 1);
        tsget.setTimeRange(startTime, endTime).setMaxVersions(requestedVersions);
        Map<byte[], NavigableSet<byte[]>> kvs = get.getFamilyMap();
        for (Map.Entry<byte[], NavigableSet<byte[]>> entry : kvs.entrySet()) {
            byte[] family = entry.getKey();
            NavigableSet<byte[]> qualifiers = entry.getValue();
            if (qualifiers == null || qualifiers.isEmpty()) {
                tsget.addFamily(family);
            } else {
                for (byte[] qualifier : qualifiers) {
                    tsget.addColumn(family, qualifier);
                    tsget.addColumn(family, TransactionManager.addShadowCellSuffix(qualifier));
                }
            }
        }
        LOG.trace("Initial Get = {}", tsget);
        getsPerformed++;

        // Return the KVs that belong to the transaction snapshot, ask for more
        // versions if needed
        Result result = table.get(tsget);
        List<KeyValue> filteredKeyValues = Collections.emptyList();
        if (!result.isEmpty()) {
            filteredKeyValues = filterKeyValuesForSnapshot(result.list(), transaction, requestedVersions);
        }
        
        return new Result(filteredKeyValues);
    }

    /**
     * Deletes the specified cells/row.
     *
     * @param delete
     *            The object that specifies what to delete.
     * @throws IOException
     *             if a remote or network exception occurs.
     */
    public void delete(Transaction transaction, Delete delete) throws IOException {

        throwExceptionIfOpSetsTimerange(delete);

        final long startTimestamp = transaction.getStartTimestamp();
        boolean issueGet = false;

        final Put deleteP = new Put(delete.getRow(), startTimestamp);
        final Get deleteG = new Get(delete.getRow());
        Map<byte[], List<KeyValue>> fmap = delete.getFamilyMap();
        if (fmap.isEmpty()) {
            issueGet = true;
        }
        for (List<KeyValue> kvl : fmap.values()) {
            for (KeyValue kv : kvl) {
                throwExceptionIfTimestampSet(kv);
                switch (KeyValue.Type.codeToType(kv.getType())) {
                case DeleteColumn:
                    deleteP.add(kv.getFamily(), kv.getQualifier(), startTimestamp, DELETE_TOMBSTONE);
                    transaction.addCell(new HBaseCellIdImpl(table, delete.getRow(), kv.getFamily(), kv.getQualifier()));
                    break;
                case DeleteFamily:
                    deleteG.addFamily(kv.getFamily());
                    issueGet = true;
                    break;
                case Delete:
                    if (kv.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
                        deleteP.add(kv.getFamily(), kv.getQualifier(), startTimestamp, DELETE_TOMBSTONE);
                        transaction.addCell(new HBaseCellIdImpl(table, delete.getRow(),
                                kv.getFamily(), kv.getQualifier()));
                        break;
                    } else {
                        throw new UnsupportedOperationException(
                                "Cannot delete specific versions on Snapshot Isolation.");
                    }
                }
            }
        }
        if (issueGet) {
            // It's better to perform a transactional get to avoid deleting more
            // than necessary
            Result result = this.get(transaction, deleteG);
            if (!result.isEmpty()) {
                for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entryF : result.getMap()
                        .entrySet()) {
                    byte[] family = entryF.getKey();
                    for (Entry<byte[], NavigableMap<Long, byte[]>> entryQ : entryF.getValue().entrySet()) {
                        byte[] qualifier = entryQ.getKey();
                        deleteP.add(family, qualifier, DELETE_TOMBSTONE);
                        transaction.addCell(new HBaseCellIdImpl(table, delete.getRow(), family, qualifier));
                    }
                }
            }
        }

        table.put(deleteP);
    }

    /**
     * Puts some data in the table.
     * <p>
     * If {@link #isAutoFlush isAutoFlush} is false, the update is buffered
     * until the internal buffer is full.
     *
     * @param put
     *            The data to put.
     * @throws IOException
     *             if a remote or network exception occurs.
     * @since 0.20.0
     */
    public void put(Transaction transaction, Put put) throws IOException {

        throwExceptionIfOpSetsTimerange(put);

        final long startTimestamp = transaction.getStartTimestamp();
        // create put with correct ts
        final Put tsput = new Put(put.getRow(), startTimestamp);
        Map<byte[], List<KeyValue>> kvs = put.getFamilyMap();
        for (List<KeyValue> kvl : kvs.values()) {
            for (KeyValue kv : kvl) {
                throwExceptionIfTimestampSet(kv);
                tsput.add(new KeyValue(kv.getRow(), kv.getFamily(), kv.getQualifier(), startTimestamp, kv.getValue()));
                transaction.addCell(new HBaseCellIdImpl(table, kv.getRow(), kv.getFamily(), kv.getQualifier()));
            }
        }

        table.put(tsput);
    }

    /**
     * Returns a scanner on the current table as specified by the {@link Scan}
     * object. Note that the passed {@link Scan}'s start row and caching
     * properties maybe changed.
     *
     * @param scan
     *            A configured {@link Scan} object.
     * @return A scanner.
     * @throws IOException
     *             if a remote or network exception occurs.
     */
    public ResultScanner getScanner(Transaction transaction, Scan scan) throws IOException {

        throwExceptionIfOpSetsTimerange(scan);

        Scan tsscan = new Scan(scan);
        tsscan.setMaxVersions((int) (versionsAvg + CACHE_VERSIONS_OVERHEAD));
        tsscan.setTimeRange(0, transaction.getStartTimestamp() + 1);
        Map<byte[], NavigableSet<byte[]>> kvs = tsscan.getFamilyMap();
        for (Map.Entry<byte[], NavigableSet<byte[]>> entry : kvs.entrySet()) {
            byte[] family = entry.getKey();
            NavigableSet<byte[]> qualifiers = entry.getValue();
            if (qualifiers == null) {
                continue;
            }
            for (byte[] qualifier : qualifiers) {
                tsscan.addColumn(family, TransactionManager.addShadowCellSuffix(qualifier));
            }
        }
        TransactionalClientScanner scanner = new TransactionalClientScanner(transaction,
                tsscan, (int) (versionsAvg + CACHE_VERSIONS_OVERHEAD));
        return scanner;
    }

    @ThreadSafe
    static class IterableColumn implements Iterable<List<KeyValue>> {

        @ThreadSafe
        class ColumnIterator implements Iterator<List<KeyValue>> {

            private final Iterator<ColumnWrapper> listIterator = columnList.listIterator();

            @Override
            public boolean hasNext() {
                return listIterator.hasNext();
            }

            @Override
            public List<KeyValue> next() {
                ColumnWrapper columnWrapper = listIterator.next();
                return columns.get(columnWrapper);
            }

            @Override
            public void remove() {
                // Not Implemented
            }

        }

        private final Map<ColumnWrapper, List<KeyValue>> columns = new HashMap<ColumnWrapper, List<KeyValue>>();
        private final List<ColumnWrapper> columnList = new ArrayList<ColumnWrapper>();

        public IterableColumn(List<KeyValue> keyValues) {
            for (KeyValue kv : keyValues) {
                if (TransactionManager.isShadowCell(kv.getQualifier())) {
                    continue;
                }
                ColumnWrapper currentColumn = new ColumnWrapper(kv.getFamily(), kv.getQualifier());
                if (!columns.containsKey(currentColumn)) {
                    columns.put(currentColumn, new ArrayList<KeyValue>(Arrays.asList(kv)));
                    columnList.add(currentColumn);
                } else {
                    List<KeyValue> columnKeyValues = columns.get(currentColumn);
                    columnKeyValues.add(kv);
                }
            }

        }

        @Override
        public Iterator<List<KeyValue>> iterator() {
            return new ColumnIterator();
        }

    }

    /**
     * Filters the raw results returned from HBase and returns only those
     * belonging to the current snapshot, as defined by the transaction
     * object. If the raw results don't contain enough information for a
     * particular qualifier, it will request more versions from HBase.
     *
     * @param rawKeyValues
     *            Raw KVs that we are going to filter
     * @param transaction
     *            Defines the current snapshot
     * @param versionsToRequest
     *            Number of versions requested from hbase
     * @return Filtered KVs belonging to the transaction snapshot
     * @throws IOException
     */
    List<KeyValue> filterKeyValuesForSnapshot(List<KeyValue> rawKeyValues, Transaction transaction,
            int versionsToRequest) throws IOException {

        assert (rawKeyValues != null && transaction != null && versionsToRequest >= 1);

        List<KeyValue> keyValuesInSnapshot = new ArrayList<KeyValue>();
        List<Get> pendingGetsList = new ArrayList<Get>();

        int numberOfVersionsToFetch = versionsToRequest * 2 + CACHE_VERSIONS_OVERHEAD;

        Map<Long, Long> commitCache = buildCommitCache(rawKeyValues);

        for (List<KeyValue> columnKeyValues : new IterableColumn(rawKeyValues)) {
            int versionsProcessed = 0;
            boolean snapshotValueFound = false;
            KeyValue oldestKV = null;
            for (KeyValue kv : columnKeyValues) {
                if (isKeyValueInSnapshot(kv, transaction, commitCache)) {
                    if (!Arrays.equals(kv.getValue(), DELETE_TOMBSTONE)) {
                        keyValuesInSnapshot.add(kv);
                    }
                    snapshotValueFound = true;
                    break;
                }
                oldestKV = kv;
                versionsProcessed++;
            }
            if (!snapshotValueFound) {
                assert (oldestKV != null);
                Get pendingGet = createPendingGet(oldestKV, numberOfVersionsToFetch);
                pendingGetsList.add(pendingGet);
            }
            updateAvgNumberOfVersionsToFetchFromHBase(versionsProcessed);
        }

        if (!pendingGetsList.isEmpty()) {
            Result[] pendingGetsResults = table.get(pendingGetsList);
            for (Result pendingGetResult : pendingGetsResults) {
                if (!pendingGetResult.isEmpty()) {
                    keyValuesInSnapshot.addAll(
                            filterKeyValuesForSnapshot(pendingGetResult.list(), transaction, numberOfVersionsToFetch));
                }
            }
        }

        Collections.sort(keyValuesInSnapshot, KeyValue.COMPARATOR);

        assert (keyValuesInSnapshot.size() <= rawKeyValues.size());
        return keyValuesInSnapshot;
    }

    private Map<Long, Long> buildCommitCache(List<KeyValue> rawKeyValues) {

        Map<Long, Long> commitCache = new HashMap<Long, Long>();

        for (KeyValue kv : rawKeyValues) {
            if (TransactionManager.isShadowCell(kv.getQualifier())) {
                commitCache.put(kv.getTimestamp(), Bytes.toLong(kv.getValue()));
            }
        }

        return commitCache;
    }

    private boolean isKeyValueInSnapshot(KeyValue kv, Transaction transaction, Map<Long, Long> commitCache)
            throws IOException {

        long startTimestamp = transaction.getStartTimestamp();

        if (kv.getTimestamp() == startTimestamp) {
            return true;
        }

        Optional<Long> commitTimestamp = Optional.absent();
        try {
            commitTimestamp = checkAndGetCommitTimestamp(transaction, kv, commitCache);
        } catch (ExecutionException ee) {
            throw new IOException("Error reading commit timestamp", ee.getCause());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted reading commit timestamp", ie);
        }

        return commitTimestamp.isPresent() && commitTimestamp.get() < startTimestamp;
    }

    private Get createPendingGet(KeyValue kv, int versionCount) throws IOException {

        Get pendingGet = new Get(kv.getRow());
        pendingGet.addColumn(kv.getFamily(), kv.getQualifier());
        pendingGet.addColumn(kv.getFamily(), TransactionManager.addShadowCellSuffix(kv.getQualifier()));
        pendingGet.setMaxVersions(versionCount);
        pendingGet.setTimeRange(0, kv.getTimestamp());

        return pendingGet;
    }

    // TODO Try to avoid to use the versionsAvg gloval attribute in here
    private void updateAvgNumberOfVersionsToFetchFromHBase(int versionsProcessed) {

        if (versionsProcessed > versionsAvg) {
            versionsAvg = versionsProcessed;
        } else {
            versionsAvg = ALPHA * versionsAvg + (1 - ALPHA) * versionsProcessed;
        }

    }

    private Optional<Long> checkAndGetCommitTimestamp(Transaction transaction, KeyValue kv, Map<Long, Long> commitCache)
            throws InterruptedException, ExecutionException, IOException {
        long startTimestamp = kv.getTimestamp();
        if (commitCache.containsKey(startTimestamp)) {
            return Optional.of(commitCache.get(startTimestamp));
        }
        Future<Optional<Long>> f = transaction.tsoclient.getCommitTimestamp(startTimestamp);
        Optional<Long> commitTimestamp = f.get();
        if (commitTimestamp.isPresent()) {
            // If the commit timestamp is found in the persisted commit table,
            // that means the writing process of the shadow cell in the post
            // commit phase of the client probably failed, so we heal the shadow
            // cell with the right commit timestamp for avoiding further reads to
            // hit the storage
            healShadowCell(kv, commitTimestamp.get());
            return commitTimestamp;
        }
        Get get = new Get(kv.getRow());
        byte[] family = kv.getFamily();
        byte[] shadowCellQualifier = TransactionManager.addShadowCellSuffix(kv.getQualifier());
        get.addColumn(family, shadowCellQualifier);
        get.setMaxVersions(1);
        get.setTimeStamp(startTimestamp);
        Result result = table.get(get);
        if (result.containsColumn(family, shadowCellQualifier)) {
            return Optional.of(Bytes.toLong(result.getValue(family, shadowCellQualifier)));
        }
        return Optional.absent();
    }

    void healShadowCell(KeyValue kv, long commitTimestamp) {
        Runnable shadowCellHealerTask = new ShadowCellsHealerTask(kv, commitTimestamp);
        shadowCellHealerExecutor.execute(shadowCellHealerTask);
    }

    protected class TransactionalClientScanner implements ResultScanner {
        private Transaction state;
        private ResultScanner innerScanner;
        private int maxVersions;

        TransactionalClientScanner(Transaction state, Scan scan, int maxVersions)
                throws IOException {
            this.state = state;
            this.innerScanner = table.getScanner(scan);
            this.maxVersions = maxVersions;
        }


        @Override
        public Result next() throws IOException {
            List<KeyValue> filteredResult = Collections.emptyList();
            while (filteredResult.isEmpty()) {
                Result result = innerScanner.next();
                if (result == null) {
                    return null;
                }
                if (!result.isEmpty()) {
                    filteredResult = filterKeyValuesForSnapshot(result.list(), state, maxVersions);
                }
            }
            return new Result(filteredResult);
        }

        // In principle no need to override, copied from super.next(int) to make
        // sure it works even if super.next(int)
        // changes its implementation
        @Override
        public Result[] next(int nbRows) throws IOException {
            // Collect values to be returned here
            ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
            for (int i = 0; i < nbRows; i++) {
                Result next = next();
                if (next != null) {
                    resultSets.add(next);
                } else {
                    break;
                }
            }
            return resultSets.toArray(new Result[resultSets.size()]);
        }

        @Override
        public void close() {
            innerScanner.close();
        }

        @Override
        public Iterator<Result> iterator() {
            return innerScanner.iterator();
        }
    }

    /**
     * Gets the name of this table.
     *
     * @return the table name.
     */
    public byte[] getTableName() {
        return table.getTableName();
    }

    /**
     * Returns the {@link Configuration} object used by this instance.
     * <p>
     * The reference returned is not a copy, so any change made to it will
     * affect this instance.
     */
    public Configuration getConfiguration() {
        return table.getConfiguration();
    }

    /**
     * Gets the {@link HTableDescriptor table descriptor} for this table.
     *
     * @throws IOException
     *             if a remote or network exception occurs.
     */
    public HTableDescriptor getTableDescriptor() throws IOException {
        return table.getTableDescriptor();
    }

    /**
     * Test for the existence of columns in the table, as specified in the Get.
     * <p>
     *
     * This will return true if the Get matches one or more keys, false if not.
     * <p>
     *
     * This is a server-side call so it prevents any data from being transfered
     * to the client.
     *
     * @param get
     *            the Get
     * @return true if the specified Get matches one or more keys, false if not
     * @throws IOException
     *             e
     */
    public boolean exists(Transaction transaction, Get get) throws IOException {
        Result result = get(transaction, get);
        return !result.isEmpty();
    }

    /*
     * @Override public void batch(Transaction transaction, List<? extends Row>
     * actions, Object[] results) throws IOException, InterruptedException { //
     * TODO Auto-generated method stub
     * 
     * }
     * 
     * @Override public Object[] batch(Transaction transaction, List<? extends
     * Row> actions) throws IOException, InterruptedException { // TODO
     * Auto-generated method stub return null; }
     * 
     * @Override public <R> void batchCallback(Transaction transaction, List<?
     * extends Row> actions, Object[] results, Callback<R> callback) throws
     * IOException, InterruptedException { // TODO Auto-generated method stub
     * 
     * }
     * 
     * @Override public <R> Object[] batchCallback(List<? extends Row> actions,
     * Callback<R> callback) throws IOException, InterruptedException { // TODO
     * Auto-generated method stub return null; }
     */

    /**
     * Extracts certain cells from the given rows, in batch.
     *
     * @param gets
     *            The objects that specify what data to fetch and from which
     *            rows.
     *
     * @return The data coming from the specified rows, if it exists. If the row
     *         specified doesn't exist, the {@link Result} instance returned
     *         won't contain any {@link KeyValue}, as indicated by
     *         {@link Result#isEmpty()}. If there are any failures even after
     *         retries, there will be a null in the results array for those
     *         Gets, AND an exception will be thrown.
     * @throws IOException
     *             if a remote or network exception occurs.
     *
     */
    public Result[] get(Transaction transaction, List<Get> gets) throws IOException {
        Result[] results = new Result[gets.size()];
        int i = 0;
        for (Get get : gets) {
            results[i++] = get(transaction, get);
        }
        return results;
    }

    /**
     * Gets a scanner on the current table for the given family.
     *
     * @param family
     *            The column family to scan.
     * @return A scanner.
     * @throws IOException
     *             if a remote or network exception occurs.
     */
    public ResultScanner getScanner(Transaction transaction, byte[] family) throws IOException {
        Scan scan = new Scan();
        scan.addFamily(family);
        return getScanner(transaction, scan);
    }

    /**
     * Gets a scanner on the current table for the given family and qualifier.
     *
     * @param family
     *            The column family to scan.
     * @param qualifier
     *            The column qualifier to scan.
     * @return A scanner.
     * @throws IOException
     *             if a remote or network exception occurs.
     */
    public ResultScanner getScanner(Transaction transaction, byte[] family, byte[] qualifier) throws IOException {
        Scan scan = new Scan();
        scan.addColumn(family, qualifier);
        return getScanner(transaction, scan);
    }

    /**
     * Puts some data in the table, in batch.
     * <p>
     * If {@link #isAutoFlush isAutoFlush} is false, the update is buffered
     * until the internal buffer is full.
     * <p>
     * This can be used for group commit, or for submitting user defined
     * batches. The writeBuffer will be periodically inspected while the List is
     * processed, so depending on the List size the writeBuffer may flush not at
     * all, or more than once.
     *
     * @param puts
     *            The list of mutations to apply. The batch put is done by
     *            aggregating the iteration of the Puts over the write buffer at
     *            the client-side for a single RPC call.
     * @throws IOException
     *             if a remote or network exception occurs.
     */
    public void put(Transaction transaction, List<Put> puts) throws IOException {
        for (Put put : puts) {
            put(transaction, put);
        }
    }

    /**
     * Deletes the specified cells/rows in bulk.
     *
     * @param deletes
     *            List of things to delete. List gets modified by this method
     *            (in particular it gets re-ordered, so the order in which the
     *            elements are inserted in the list gives no guarantee as to the
     *            order in which the {@link Delete}s are executed).
     * @throws IOException
     *             if a remote or network exception occurs. In that case the
     *             {@code deletes} argument will contain the {@link Delete}
     *             instances that have not be successfully applied.
     */
    public void delete(Transaction transaction, List<Delete> deletes) throws IOException {
        for (Delete delete : deletes) {
            delete(transaction, delete);
        }
    }

    /**
     * Provides access to the underliying HTable in order to configure it or to
     * perform unsafe (non-transactional) operations. The latter would break the
     * transactional guarantees of the whole system.
     *
     * @return The underlying HTable object
     */
    public HTableInterface getHTable() {
        return table;
    }

    /**
     * Releases any resources held or pending changes in internal buffers.
     *
     * @throws IOException
     *             if a remote or network exception occurs.
     */
    public void close() throws IOException {
        table.close();
        shadowCellHealerExecutor.shutdown();
        try {
            if (!shadowCellHealerExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.error("Healer executor did not finish in 10s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        healerTable.close();
    }

    /**
     * Turns 'auto-flush' on or off.
     *
     * When enabled (default), Put operations don't get buffered/delayed and are immediately executed.
     *
     * Turning off autoFlush means that multiple Puts will be accepted before any RPC is actually sent to do the write
     * operations. Writes will still be automatically flushed at commit time, so no data will be lost.
     *
     * @param autoFlush
     *            Whether or not to enable 'auto-flush'.
     */
    public void setAutoFlush(boolean autoFlush) {
        table.setAutoFlush(autoFlush, true);
    }

    /**
     * Tells whether or not 'auto-flush' is turned on.
     *
     * @return true if 'auto-flush' is enabled (default), meaning Put operations don't get buffered/delayed and are
     *         immediately executed.
     */
    public boolean isAutoFlush() {
        return table.isAutoFlush();
    }

    /**
     * {@link HTable.getWriteBufferSize}
     */
    public long getWriteBufferSize() {
        return table.getWriteBufferSize();
    }

    /**
     * {@link HTable.setWriteBufferSize}
     */
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        table.setWriteBufferSize(writeBufferSize);
    }

    public void flushCommits() throws IOException{
        table.flushCommits();
    }

    // ****************************************************************************************************************
    //
    // Helper methods
    //
    // ****************************************************************************************************************

    private void throwExceptionIfOpSetsTimerange(Get getOperation) {
        TimeRange tr = getOperation.getTimeRange();
        checkTimerangeIsSetToDefaultValuesOrThrowException(tr);
    }

    private void throwExceptionIfOpSetsTimerange(Scan scanOperation) {
        TimeRange tr = scanOperation.getTimeRange();
        checkTimerangeIsSetToDefaultValuesOrThrowException(tr);
    }

    private void checkTimerangeIsSetToDefaultValuesOrThrowException(TimeRange tr) {
        if (tr.getMin() != 0L || tr.getMax() != Long.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Timestamp/timerange not allowed in transactional user operations");
        }
    }

    private void throwExceptionIfOpSetsTimerange(Mutation userOperation) {
        if (userOperation.getTimeStamp() != HConstants.LATEST_TIMESTAMP) {
            throw new IllegalArgumentException(
                    "Timestamp not allowed in transactional user operations");
        }
    }

    private void throwExceptionIfTimestampSet(KeyValue keyValue) {
        if (keyValue.getTimestamp() != HConstants.LATEST_TIMESTAMP) {
            throw new IllegalArgumentException(
                    "Timestamp not allowed in transactional user operations");
        }
    }

}