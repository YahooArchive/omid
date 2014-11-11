package com.yahoo.omid.committable.hbase;

import static com.google.common.base.Charsets.UTF_8;
import static com.yahoo.omid.ZKConstants.OMID_NAMESPACE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerSet;
import org.apache.bookkeeper.client.LedgerSet.Entry;
import org.apache.bookkeeper.client.LedgerSet.EntryId;
import org.apache.bookkeeper.client.LedgerSet.MetadataStorage;
import org.apache.bookkeeper.client.LedgerSet.ReaderWriter;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Instances of this class, before writing the commit table entries to HBase,
 * it stores them in a log (BookKeeper in this case)
 */
public class HBaseHACommitTable extends HBaseCommitTable {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseHACommitTable.class);

    static final String HA_COMMIT_TABLE_KEY = "HACommitTable";

    /**
     * Constants related to storage in HBase of the last entry id in the log.
     * As the last entry id requires just one row in the datastore, we're
     * going to use the same column family as for the commit table data.
     */
    static final byte[] HA_LAST_ENTRY_ID_ROW = "LAST_ENTRY_ID".getBytes(UTF_8);
    public static final byte[] HA_LAST_ENTRY_ID_FAMILY = HBaseCommitTable.COMMIT_TABLE_FAMILY;
    static final byte[] HA_LAST_ENTRY_ID_QUALIFIER = "LEIQ".getBytes(UTF_8);

    static final String COMMIT_CACHE_BK_LEDGERSET_NAME = "CC_BK_LOG";

    private final BookKeeper bk;
    private final MetadataStorage metadataStore;

    /**
     * Create a HBase commit table with high availability.
     * Note that we do not take ownership of the passed htable,
     * it is just used to construct the writer and client.
     */
    @Inject
    public HBaseHACommitTable(Configuration hbaseConfig,
                            HBaseCommitTableConfig config,
                            @Named(HA_COMMIT_TABLE_KEY) BookKeeper bk,
                            @Named(HA_COMMIT_TABLE_KEY) MetadataStorage metadataStore) {

        this(hbaseConfig, config.getTableName(), defaultKeyGenerator(), bk, metadataStore);

    }

    protected HBaseHACommitTable(Configuration hbaseConfig,
                                 String tableName,
                                 KeyGenerator keygen,
                                 BookKeeper bk,
                                 MetadataStorage metadataStore) {

        super(hbaseConfig, tableName, keygen);
        this.bk = bk;
        this.metadataStore = metadataStore;

    }

    // ********************* HA related reader and writer *********************

    public class HBaseHAWriter implements Writer {

        private final HBaseWriter commitTableWriter;

        // This instance is used first to reconcile the log by reading it
        // and once the reconciliation is done, start writing to the log
        private final ReaderWriter logReaderWriter;

        private final List<TxBoundaries> logEntriesToWrite = new ArrayList<>();

        private byte[] previousLastEntryIdAsBytes = null;


        HBaseHAWriter(HBaseWriter hbaseWriter) throws IOException {

            // Setup the basic structures of this writer
            this.commitTableWriter = hbaseWriter;
            this.logReaderWriter = getBKLogReaderWriter();

            // Before accepting new requests, catch up the with previous
            // TSO state through the log
            try {
                reconcileCommitTableState();
            } catch (IOException | ExecutionException e) {
                throw new IOException("Could not reconcile with the previous TSO state", e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted when reconciling with the previous TSO state", e.getCause());
            }

        }

        private ReaderWriter getBKLogReaderWriter() throws IOException {

            try {
                return LedgerSet.newBuilder()
                                .setClient(bk)
                                .setMetadataStorage(metadataStore)
                                .buildReaderWriter(COMMIT_CACHE_BK_LEDGERSET_NAME).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted getting the ledger set reader/writer");
            } catch (ExecutionException e) {
                throw new IOException("Error getting the ledger set reader/writer", e.getCause());
            }

        }

        private void reconcileCommitTableState()
        throws IOException, InterruptedException, ExecutionException {
            LOG.info("Reconciling commit table state with previous state...");
            Optional<byte[]> logEntry = retrieveCommitTableLastLogEntry();
            readLogApplyingChangesInCommitTable(logEntry);
            LOG.info("State reconciliation finished. Current commit table state is up-to-date");
        }

        private Optional<byte[]> retrieveCommitTableLastLogEntry() throws IOException {
            try (HBaseLogPositionReader logPositionReader = new HBaseLogPositionReader()) {
                return logPositionReader.readLogPosition();
            }
        }

        private void readLogApplyingChangesInCommitTable(Optional<byte[]> readLogPosition)
        throws IOException, InterruptedException, ExecutionException {
            if (readLogPosition.isPresent()) { // Last entry was found in HBase, so skip to it in the log
                try (InputStream is = new ByteArrayInputStream(readLogPosition.get())) {
                    EntryId entryId = LedgerSet.readEntryIdFrom(is);
                    logReaderWriter.skip(entryId).get();
                }
            } else {
                LOG.info("Commit table last log entry not found. Start reading the log from scratch");
            }

            while (logReaderWriter.hasMoreEntries().get()) {
                Entry lastEntryRead = logReaderWriter.nextEntry().get();
                byte[] lastEntry = lastEntryRead.getBytes();
                ByteBuffer bb = ByteBuffer.wrap(lastEntry);
                int numberOfTxBoundaries = bb.getInt();
                LOG.info("New log entry read with {} TxBoundaries", numberOfTxBoundaries);
                for (int i = 0; i < numberOfTxBoundaries; i++) {
                    long startTimestamp = bb.getLong();
                    long commitTimestamp = bb.getLong();
                    commitTableWriter.addCommittedTransaction(startTimestamp, commitTimestamp);
                    LOG.info("Tx with boundaries [ ST {} / CT {} ] added to commit table", startTimestamp, commitTimestamp);
                }
                assert (bb.remaining() == 0);
            }
            commitTableWriter.flush().get();
        }

        // ******************* Writer interface implementation ****************
        @Override
        public void addCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException {
            logEntriesToWrite.add(new TxBoundaries(startTimestamp, commitTimestamp));
        }

        @Override
        public void updateLowWatermark(long lowWatermark) throws IOException {
            commitTableWriter.updateLowWatermark(lowWatermark);
        }

        @Override
        public ListenableFuture<Void> flush() {
            SettableFuture<Void> f = SettableFuture.<Void> create();
            byte[] lastEntryIdAsBytes = null;
            // Check if there are entries to put in the log
            if (logEntriesToWrite.size() > 0) {
                // Put first the logEntries in BK...
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    byte[] logEntriesAsByteArray = getLogEntriesToWriteAsByteArray(logEntriesToWrite);
                    EntryId lastEntryId = logReaderWriter.writeEntry(logEntriesAsByteArray).get();
                    lastEntryId.writeTo(baos);
                    lastEntryIdAsBytes = baos.toByteArray();
                } catch (IOException e) {
                    LOG.error("Error getting log entry id as bytes");
                    f.setException(e);
                    return f;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.error("Interrupted when writing entry to the log");
                    f.setException(e);
                    return f;
                } catch (ExecutionException e) {
                    LOG.error("Error writing entry to the log");
                    f.setException(e);
                    return f;
                }
                // ...and then write the batch to the datastore...
                try {
                    for (TxBoundaries txBoundaries : logEntriesToWrite) {
                        commitTableWriter.addCommittedTransaction(txBoundaries.startTimestamp,
                                                                  txBoundaries.commitTimestamp);
                    }
                } catch (IOException e) {
                    LOG.error("Error whilst writing committed transactions to commit table", e);
                    f.setException(e);
                    return f;
                }
                // ...storing the previous log position also in the datastore if necessary...
                if (previousLastEntryIdAsBytes != null) {
                    // This means that we put some entries in the log, so
                    // update the previous last EntryId in the BK column
                    Put put = new Put(HA_LAST_ENTRY_ID_ROW);
                    put.add(HA_LAST_ENTRY_ID_FAMILY, HA_LAST_ENTRY_ID_QUALIFIER, previousLastEntryIdAsBytes);
                    try {
                        commitTableWriter.table.put(put);
                    } catch (IOException e) {
                        LOG.error("Error writing previous last entry id data", e);
                        f.setException(e);
                        return f;
                    }
                }

                // ... and finally flush to the datastore, swap the last entry written & clear entries to write
                f = (SettableFuture<Void>) commitTableWriter.flush();
                try {
                    f.get(); // Ensure changes have been flushed
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.error("Interrupted when flushing to the log");
                    f.setException(e);
                    return f;
                } catch (ExecutionException e) {
                    LOG.error("Error flushing to the log");
                    f.setException(e);
                    return f;
                }
                previousLastEntryIdAsBytes = lastEntryIdAsBytes;
                logEntriesToWrite.clear();
            } else {
                // If there are no elements to write in the batch, just set the future to void
                f.set(null);
            }

            return f;
        }

        @Override
        public void close() throws IOException {
            commitTableWriter.close();
            logReaderWriter.close();
        }

        private byte[] getLogEntriesToWriteAsByteArray(List<TxBoundaries> logEntriesToWrite) {
            // Bytes to reseve in the returned byte array:
            // sizeof(Int) = # of Entries to write
            // +
            // (# of Entries to write * ( sizeof(Long) = Start Timestamp + sizeof(Long) = Commit TS ) )
            int logEntriesToWriteCount = logEntriesToWrite.size();
            ByteBuffer bb = ByteBuffer.allocate(4 + ((logEntriesToWriteCount * 2 * Long.SIZE) / 8));
            bb.putInt(logEntriesToWriteCount);
            for (TxBoundaries txBoundaries : logEntriesToWrite) {
                bb.putLong(txBoundaries.startTimestamp);
                bb.putLong(txBoundaries.commitTimestamp);
            }
            return bb.array();
        }

    }

    public class HBaseLogPositionReader implements Closeable {

        private final HTable table;

        HBaseLogPositionReader() throws IOException {
            table = new HTable(hbaseConfig, tableName);
        }

        public Optional<byte[]> readLogPosition() throws IOException {
            Optional<byte[]> logPosition = Optional.absent(); // Default value means its not found
            Get get = new Get(HA_LAST_ENTRY_ID_ROW);
            get.addColumn(HA_LAST_ENTRY_ID_FAMILY, HA_LAST_ENTRY_ID_QUALIFIER);
            Result result = table.get(get);
            if (containsLastEntryId(result)) {
                logPosition = Optional.of(result.getValue(HA_LAST_ENTRY_ID_FAMILY, HA_LAST_ENTRY_ID_QUALIFIER));
            }
            return logPosition;
        }

        private boolean containsLastEntryId(Result result) {
            return (result != null
                    &&
                    result.containsColumn(HA_LAST_ENTRY_ID_FAMILY, HA_LAST_ENTRY_ID_QUALIFIER));
        }

        @Override
        public void close() throws IOException {
            table.close();
        }

    }

    // ******************************* Getters ********************************

    @Override
    public ListenableFuture<Writer> getWriter() {
        SettableFuture<Writer> f = SettableFuture.<Writer> create();
        try {
            f.set(new HBaseHAWriter(new HBaseWriter(hbaseConfig, tableName)));
        } catch (IOException ioe) {
            f.setException(ioe);
        }
        return f;
    }

    // ************************* Helper Utilities *****************************

    public static BookKeeper provideBookKeeperClient(String zkCluster)
            throws IOException, InterruptedException, KeeperException {

        LOG.info("Creating BK client connected to ZK cluster {}...", zkCluster);

        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkServers(zkCluster);
        return new BookKeeper(conf);

    }

    public static CuratorFramework provideZookeeperClient(String zkCluster) {

        LOG.info("Creating Zookeeper Client connecting to {}", zkCluster);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return CuratorFrameworkFactory.builder()
                                      .namespace(OMID_NAMESPACE)
                                      .connectString(zkCluster)
                                      .retryPolicy(retryPolicy)
                                      .build();
    }
}
