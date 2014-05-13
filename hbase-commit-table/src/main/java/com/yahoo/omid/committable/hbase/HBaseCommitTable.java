package com.yahoo.omid.committable.hbase;

import static com.google.common.base.Charsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.yahoo.omid.committable.CommitTable;

public class HBaseCommitTable implements CommitTable {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseCommitTable.class);

    public static final String COMMIT_TABLE_DEFAULT_NAME = "OMID_COMMIT_TABLE";
    static final byte[] COMMIT_TABLE_FAMILY = "F".getBytes(UTF_8);
    static final byte[] COMMIT_TABLE_QUALIFIER = "C".getBytes(UTF_8);

    private final HTable table;

    public HBaseCommitTable(HTable table) {
        this.table = table;
        table.setAutoFlush(false, true);
    }

    public class HBaseWriter implements Writer {

        @Override
        public void addCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException {
            assert(startTimestamp < commitTimestamp);
            Put put = new Put(startTimestampToKey(startTimestamp));
            put.add(COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER,
                    encodeCommitTimestamp(startTimestamp, commitTimestamp));

            table.put(put);
        }

        @Override
        public ListenableFuture<Void> flush() {
            SettableFuture<Void> f = SettableFuture.<Void>create();
            try {
                table.flushCommits();
                f.set(null);
            } catch (IOException e) {
                LOG.error("Error flushing data", e);
                f.setException(e);
            }
            return f;
        }

    }

    public class HBaseClient implements Client {

        @Override
        public ListenableFuture<Optional<Long>> getCommitTimestamp(long startTimestamp) {

            SettableFuture<Optional<Long>> f = SettableFuture.<Optional<Long>>create();
            Result result = null;
            try {
                Get get = new Get(startTimestampToKey(startTimestamp));
                get.addColumn(COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER);
                result = table.get(get);
                if (containsATimestamp(result)) {
                    long commitTs = decodeCommitTimestamp(startTimestamp,
                            result.getValue(COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER));
                    Optional<Long> commitTsValue = Optional.of(commitTs);
                    f.set(commitTsValue);
                } else {
                    Optional<Long> absentValue = Optional.absent();
                    f.set(absentValue);
                }
            } catch (IOException e) {
                LOG.error("Error getting commit timestamp for TX {}", startTimestamp, e);
                f.setException(e);
            }
            return f;
        }

        @Override
        public ListenableFuture<Void> completeTransaction(long startTimestamp) {

            SettableFuture<Void> f = SettableFuture.<Void> create();
            try {
                Delete delete = new Delete(startTimestampToKey(startTimestamp));
                table.delete(delete);
                f.set(null);
            } catch (IOException e) {
                LOG.error("Error removing TX {}", startTimestamp, e);
                f.setException(e);
            }
            return f;
        }

        private boolean containsATimestamp(Result result) {
            return (result != null && result.containsColumn(COMMIT_TABLE_FAMILY, COMMIT_TABLE_QUALIFIER));
        }

    }

    @Override
    public ListenableFuture<Writer> getWriter() {
        SettableFuture<Writer> f = SettableFuture.<Writer> create();
        f.set(new HBaseWriter());
        return f;
    }

    @Override
    public ListenableFuture<Client> getClient() {
        SettableFuture<Client> f = SettableFuture.<Client> create();
        f.set(new HBaseClient()); // Check this depending on (*)
        return f;
    }

    /**
     * This method allows to spread the start timestamp into different regions in HBase in order to avoid hotspot
     * regions
     */
    static byte[] startTimestampToKey(long startTimestamp) {
        assert (startTimestamp >= 0 && startTimestamp <= Long.MAX_VALUE);
        // 1) Reverse the timestamp to better spread
        long reversedStartTimestamp = Long.reverse(startTimestamp | Long.MIN_VALUE);
        // 2) Convert to a byte array with big endian format
        byte[] bytes = new byte[8];
        bytes[0] = (byte) ((reversedStartTimestamp >> 56) & 0xFF);
        bytes[1] = (byte) ((reversedStartTimestamp >> 48) & 0xFF);
        bytes[2] = (byte) ((reversedStartTimestamp >> 40) & 0xFF);
        bytes[3] = (byte) ((reversedStartTimestamp >> 32) & 0xFF);
        bytes[4] = (byte) ((reversedStartTimestamp >> 24) & 0xFF);
        bytes[5] = (byte) ((reversedStartTimestamp >> 16) & 0xFF);
        bytes[6] = (byte) ((reversedStartTimestamp >> 8) & 0xFF);
        bytes[7] = (byte) ((reversedStartTimestamp) & 0xFE);
        return bytes;
    }

    static long keyToStartTimestamp(byte[] key) {
        assert (key.length == 8);
        // 1) Convert from big endian each byte
        long startTimestamp = ((long) key[0] & 0xFF) << 56
                        | ((long) key[1] & 0xFF) << 48
                        | ((long) key[2] & 0xFF) << 40
                        | ((long) key[3] & 0xFF) << 32
                        | ((long) key[4] & 0xFF) << 24
                        | ((long) key[5] & 0xFF) << 16
                        | ((long) key[6] & 0xFF) << 8
                        | ((long) key[7] & 0xFF);
        // 2) Reverse to obtain the original value
        return Long.reverse(startTimestamp);
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
