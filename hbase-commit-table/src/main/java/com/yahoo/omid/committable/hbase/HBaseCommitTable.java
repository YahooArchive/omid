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
package com.yahoo.omid.committable.hbase;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.CommitTimestamp.Location;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Charsets.UTF_8;
import static com.yahoo.omid.committable.hbase.CommitTableConstants.COMMIT_TABLE_FAMILY;
import static com.yahoo.omid.committable.hbase.CommitTableConstants.LOW_WATERMARK_FAMILY;

public class HBaseCommitTable implements CommitTable {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseCommitTable.class);

    static final byte[] COMMIT_TABLE_QUALIFIER = "C".getBytes(UTF_8);
    private static final byte[] INVALID_TX_QUALIFIER = "IT".getBytes(UTF_8);
    static final byte[] LOW_WATERMARK_ROW = "LOW_WATERMARK".getBytes(UTF_8);
    static final byte[] LOW_WATERMARK_QUALIFIER = "LWC".getBytes(UTF_8);

    private final String tableName;
    private final Configuration hbaseConfig;
    private final KeyGenerator keygen;

    /**
     * Create a hbase commit table.
     * Note that we do not take ownership of the passed htable,
     * it is just used to construct the writer and client.
     */
    @Inject
    public HBaseCommitTable(Configuration hbaseConfig, HBaseCommitTableConfig config) {
        this(hbaseConfig, config.getTableName(), KeyGeneratorImplementations.defaultKeyGenerator());
    }

    protected HBaseCommitTable(Configuration hbaseConfig, String tableName, KeyGenerator keygen) {

        this.hbaseConfig = hbaseConfig;
        this.tableName = tableName;
        this.keygen = keygen;

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Reader and Writer
    // ----------------------------------------------------------------------------------------------------------------

    public class HBaseWriter implements Writer {

        private static final long INITIAL_LWM_VALUE = -1L;
        final HTable table;
        // Our own buffer for operations
        final List<Put> writeBuffer = new LinkedList<>();
        final AtomicLong lowWatermarkToStore = new AtomicLong(INITIAL_LWM_VALUE);

        HBaseWriter(Configuration hbaseConfig, String tableName) throws IOException {
            table = new HTable(hbaseConfig, tableName);
        }

        // Writer Implementation

        @Override
        public void addCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException {
            assert (startTimestamp < commitTimestamp);
            Put put = new Put(startTimestampToKey(startTimestamp), startTimestamp);
            put.add(COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER, encodeCommitTimestamp(startTimestamp, commitTimestamp));
            writeBuffer.add(put);
        }

        @Override
        public void updateLowWatermark(long lowWatermark) throws IOException {
            lowWatermarkToStore.set(lowWatermark);
        }

        @Override
        public void flush() throws IOException {
            try {
                addLowWatermarkToStoreToWriteBuffer();
                table.put(writeBuffer);
                writeBuffer.clear();
            } catch (IOException e) {
                LOG.error("Error flushing data", e);
                throw e;
            }
        }

        @Override
        public void clearWriteBuffer() {
            writeBuffer.clear();
        }

        @Override
        public void close() throws IOException {
            clearWriteBuffer();
            table.close();
        }

        private void addLowWatermarkToStoreToWriteBuffer() {
            long lowWatermark = lowWatermarkToStore.get();
            if(lowWatermark != INITIAL_LWM_VALUE) {
                Put put = new Put(LOW_WATERMARK_ROW);
                put.add(LOW_WATERMARK_FAMILY, LOW_WATERMARK_QUALIFIER, Bytes.toBytes(lowWatermark));
                writeBuffer.add(put);
            }
        }
    }

    public class HBaseClient implements Client, Runnable {
        final HTable table;
        final HTable deleteTable;
        final ExecutorService deleteBatchExecutor;
        final BlockingQueue<DeleteRequest> deleteQueue;
        boolean isClosed = false; // @GuardedBy("this")
        final static int DELETE_BATCH_SIZE = 1024;

        HBaseClient(Configuration hbaseConfig, String tableName) throws IOException {
            table = new HTable(hbaseConfig, tableName);
            table.setAutoFlush(false, true);
            deleteTable = new HTable(hbaseConfig, tableName);
            deleteQueue = new ArrayBlockingQueue<>(DELETE_BATCH_SIZE);

            deleteBatchExecutor = Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder().setNameFormat("omid-completor-%d").build());
            deleteBatchExecutor.submit(this);

        }

        @Override
        public ListenableFuture<Optional<CommitTimestamp>> getCommitTimestamp(long startTimestamp) {

            SettableFuture<Optional<CommitTimestamp>> f = SettableFuture.create();
            try {
                Get get = new Get(startTimestampToKey(startTimestamp));
                get.addColumn(COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER);
                get.addColumn(COMMIT_TABLE_FAMILY, INVALID_TX_QUALIFIER);

                Result result = table.get(get);

                if (containsInvalidTransaction(result)) {
                    CommitTimestamp invalidCT =
                            new CommitTimestamp(Location.COMMIT_TABLE, INVALID_TRANSACTION_MARKER, false);
                    f.set(Optional.of(invalidCT));
                    return f;
                }

                if (containsATimestamp(result)) {
                    long commitTSValue = decodeCommitTimestamp(startTimestamp,
                            result.getValue(COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER));
                    CommitTimestamp validCT =
                            new CommitTimestamp(Location.COMMIT_TABLE, commitTSValue, true);
                    f.set(Optional.of(validCT));
                } else {
                    f.set(Optional.<CommitTimestamp>absent());
                }
            } catch (IOException e) {
                LOG.error("Error getting commit timestamp for TX {}", startTimestamp, e);
                f.setException(e);
            }
            return f;
        }

        @Override
        public ListenableFuture<Long> readLowWatermark() {
            SettableFuture<Long> f = SettableFuture.create();
            try {
                Get get = new Get(LOW_WATERMARK_ROW);
                get.addColumn(LOW_WATERMARK_FAMILY, LOW_WATERMARK_QUALIFIER);
                Result result = table.get(get);
                if (containsLowWatermark(result)) {
                    long lowWatermark = Bytes.toLong(result.getValue(LOW_WATERMARK_FAMILY, LOW_WATERMARK_QUALIFIER));
                    f.set(lowWatermark);
                } else {
                    f.set(0L);
                }
            } catch (IOException e) {
                LOG.error("Error getting low watermark", e);
                f.setException(e);
            }
            return f;
        }

        @Override
        public ListenableFuture<Void> completeTransaction(long startTimestamp) {
            try {
                synchronized (this) {

                    if (isClosed) {
                        SettableFuture<Void> f = SettableFuture.create();
                        f.setException(new IOException("Not accepting requests anymore"));
                        return f;
                    }

                    DeleteRequest req = new DeleteRequest(
                            new Delete(startTimestampToKey(startTimestamp), startTimestamp));
                    deleteQueue.put(req);
                    return req;
                }
            } catch (IOException ioe) {
                LOG.warn("Error generating timestamp for transaction completion", ioe);
                SettableFuture<Void> f = SettableFuture.create();
                f.setException(ioe);
                return f;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                SettableFuture<Void> f = SettableFuture.create();
                f.setException(ie);
                return f;
            }
        }

        @Override
        public ListenableFuture<Boolean> tryInvalidateTransaction(long startTimestamp) {
            SettableFuture<Boolean> f = SettableFuture.create();
            try {
                byte[] row = startTimestampToKey(startTimestamp);
                Put invalidationPut = new Put(row, startTimestamp);
                invalidationPut.add(COMMIT_TABLE_FAMILY, INVALID_TX_QUALIFIER, null);

                // We need to write to the invalid column only if the commit timestamp
                // is empty. This has to be done atomically. Otherwise, if we first
                // check the commit timestamp and right before the invalidation a commit
                // timestamp is added and read by a transaction, then snapshot isolation
                // might not be hold (due to the invalidation)
                // TODO: Decide what we should we do if we can not contact the commit table. loop till succeed???
                boolean result = table.checkAndPut(row, COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER, null, invalidationPut);
                f.set(result);
            } catch (IOException ioe) {
                f.setException(ioe);
            }
            return f;
        }

        @Override
        public void run() {
            List<DeleteRequest> reqbatch = new ArrayList<>();
            try {
                while (true) {
                    DeleteRequest r = deleteQueue.poll();
                    if (r == null && reqbatch.size() == 0) {
                        r = deleteQueue.take();
                    }

                    if (r != null) {
                        reqbatch.add(r);
                    }

                    if (r == null || reqbatch.size() == DELETE_BATCH_SIZE) {
                        List<Delete> deletes = new ArrayList<>();
                        for (DeleteRequest dr : reqbatch) {
                            deletes.add(dr.getDelete());
                        }
                        try {
                            deleteTable.delete(deletes);
                            for (DeleteRequest dr : reqbatch) {
                                dr.complete();
                            }
                        } catch (IOException ioe) {
                            LOG.warn("Error contacting hbase", ioe);
                            for (DeleteRequest dr : reqbatch) {
                                dr.error(ioe);
                            }
                        } finally {
                            reqbatch.clear();
                        }
                    }
                }
            } catch (InterruptedException ie) {
                // Drain the queue and place the exception in the future
                // for those who placed requests
                LOG.warn("Draining delete queue");
                DeleteRequest queuedRequest = deleteQueue.poll();
                while (queuedRequest != null) {
                    reqbatch.add(queuedRequest);
                    queuedRequest = deleteQueue.poll();
                }
                for (DeleteRequest dr : reqbatch) {
                    dr.error(new IOException("HBase CommitTable is going to be closed"));
                }
                reqbatch.clear();
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                LOG.error("Transaction completion thread threw exception", t);
            }
        }

        @Override
        public synchronized void close() throws IOException {
            isClosed = true;
            deleteBatchExecutor.shutdownNow(); // may need to interrupt take
            try {
                if (!deleteBatchExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOG.warn("Delete executor did not shutdown");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

            LOG.warn("Re-Draining delete queue just in case");
            DeleteRequest queuedRequest = deleteQueue.poll();
            while (queuedRequest != null) {
                queuedRequest.error(new IOException("HBase CommitTable is going to be closed"));
                queuedRequest = deleteQueue.poll();
            }

            deleteTable.close();
            table.close();
        }

        private boolean containsATimestamp(Result result) {
            return (result != null && result.containsColumn(COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER));
        }

        private boolean containsInvalidTransaction(Result result) {
            return (result != null && result.containsColumn(COMMIT_TABLE_FAMILY, INVALID_TX_QUALIFIER));
        }

        private boolean containsLowWatermark(Result result) {
            return (result != null && result.containsColumn(LOW_WATERMARK_FAMILY, LOW_WATERMARK_QUALIFIER));
        }

        private class DeleteRequest extends AbstractFuture<Void> {
            final Delete delete;

            DeleteRequest(Delete delete) {
                this.delete = delete;
            }

            void error(IOException ioe) {
                setException(ioe);
            }

            void complete() {
                set(null);
            }

            Delete getDelete() {
                return delete;
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    public Writer getWriter() throws IOException {
        return new HBaseWriter(hbaseConfig, tableName);
    }

    @Override
    public Client getClient() throws IOException {
        return new HBaseClient(hbaseConfig, tableName);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    protected byte[] startTimestampToKey(long startTimestamp) throws IOException {
        return keygen.startTimestampToKey(startTimestamp);
    }

    private static byte[] encodeCommitTimestamp(long startTimestamp, long commitTimestamp) throws IOException {
        assert (startTimestamp < commitTimestamp);
        long diff = commitTimestamp - startTimestamp;
        byte[] bytes = new byte[CodedOutputStream.computeInt64SizeNoTag(diff)];
        CodedOutputStream cos = CodedOutputStream.newInstance(bytes);
        cos.writeInt64NoTag(diff);
        cos.flush();
        return bytes;

    }

    private static long decodeCommitTimestamp(long startTimestamp, byte[] encodedCommitTimestamp) throws IOException {
        CodedInputStream cis = CodedInputStream.newInstance(encodedCommitTimestamp);
        long diff = cis.readInt64();
        return startTimestamp + diff;
    }

}
