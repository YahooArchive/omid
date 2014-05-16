package com.yahoo.omid.committable.hbase;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.net.InetSocketAddress;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.yahoo.omid.committable.CommitTable;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

public class HBaseCommitTable implements CommitTable {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseCommitTable.class);

    public static final String COMMIT_TABLE_DEFAULT_NAME = "OMID_COMMIT_TABLE";
    static final byte[] COMMIT_TABLE_FAMILY = "F".getBytes(UTF_8);
    static final byte[] COMMIT_TABLE_QUALIFIER = "C".getBytes(UTF_8);

    private final HTable table;
    private final KeyGenerator keygen;


    public HBaseCommitTable(HTable table) {
        this(table, new BucketKeyGenerator());
    }

    private HBaseCommitTable(HTable table, KeyGenerator keygen) {
        this.table = table;
        table.setAutoFlush(false, true);
        this.keygen = keygen;
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

    /**
     * Implementations of this interface determine how keys are spread in HBase
     */
    interface KeyGenerator {
        byte[] startTimestampToKey(long startTimestamp) throws IOException;
        long keyToStartTimestamp(byte[] key) throws IOException;
    }

    /**
     * This is the used implementation
     */
    static class BucketKeyGenerator implements KeyGenerator {
        @Override
        public byte[] startTimestampToKey(long startTimestamp) throws IOException {
            byte[] bytes = new byte[9];
            bytes[0] = (byte) (startTimestamp & 0x0F);
            bytes[1] = (byte) ((startTimestamp >> 56) & 0xFF);
            bytes[2] = (byte) ((startTimestamp >> 48) & 0xFF);
            bytes[3] = (byte) ((startTimestamp >> 40) & 0xFF);
            bytes[4] = (byte) ((startTimestamp >> 32) & 0xFF);
            bytes[5] = (byte) ((startTimestamp >> 24) & 0xFF);
            bytes[6] = (byte) ((startTimestamp >> 16) & 0xFF);
            bytes[7] = (byte) ((startTimestamp >> 8) & 0xFF);
            bytes[8] = (byte) ((startTimestamp) & 0xFF);
            return bytes;
        }

        @Override
        public long keyToStartTimestamp(byte[] key) {
            assert (key.length == 9);
            return ((long) key[1] & 0xFF) << 56
                    | ((long) key[2] & 0xFF) << 48
                    | ((long) key[3] & 0xFF) << 40
                    | ((long) key[4] & 0xFF) << 32
                    | ((long) key[5] & 0xFF) << 24
                    | ((long) key[6] & 0xFF) << 16
                    | ((long) key[7] & 0xFF) << 8
                    | ((long) key[8] & 0xFF);
        }

    }

    static class FullRandomKeyGenerator implements KeyGenerator {
        @Override
        public byte[] startTimestampToKey(long startTimestamp) {
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

        @Override
        public long keyToStartTimestamp(byte[] key) {
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
    }

    static class BadRandomKeyGenerator implements KeyGenerator {
        @Override
        public byte[] startTimestampToKey(long startTimestamp) throws IOException {
            long reversedStartTimestamp = Long.reverse(startTimestamp);
            byte[] bytes = new byte[CodedOutputStream.computeSFixed64SizeNoTag(reversedStartTimestamp)];
            CodedOutputStream cos = CodedOutputStream.newInstance(bytes);
            cos.writeSFixed64NoTag(reversedStartTimestamp);
            cos.flush();
            return bytes;
        }

        @Override
        public long keyToStartTimestamp(byte[] key) throws IOException {
            CodedInputStream cis = CodedInputStream.newInstance(key);
            return Long.reverse(cis.readSFixed64());
        }

    }


    static class SeqKeyGenerator implements KeyGenerator {
        @Override
        public byte[] startTimestampToKey(long startTimestamp) throws IOException {
            // Convert to a byte array with big endian format
            byte[] bytes = new byte[8];
           
            bytes[0] = (byte) ((startTimestamp >> 56) & 0xFF);
            bytes[1] = (byte) ((startTimestamp >> 48) & 0xFF);
            bytes[2] = (byte) ((startTimestamp >> 40) & 0xFF);
            bytes[3] = (byte) ((startTimestamp >> 32) & 0xFF);
            bytes[4] = (byte) ((startTimestamp >> 24) & 0xFF);
            bytes[5] = (byte) ((startTimestamp >> 16) & 0xFF);
            bytes[6] = (byte) ((startTimestamp >> 8) & 0xFF);
            bytes[7] = (byte) ((startTimestamp) & 0xFF);
            return bytes;
        }

        @Override
        public long keyToStartTimestamp(byte[] key) {
            assert (key.length == 8);
            // Convert from big endian each byte
            return ((long) key[0] & 0xFF) << 56
                    | ((long) key[1] & 0xFF) << 48
                    | ((long) key[2] & 0xFF) << 40
                    | ((long) key[3] & 0xFF) << 32
                    | ((long) key[4] & 0xFF) << 24
                    | ((long) key[5] & 0xFF) << 16
                    | ((long) key[6] & 0xFF) << 8
                    | ((long) key[7] & 0xFF);
        }

    }

    static class Config {
        @Parameter(names = "-fullRandomAlgo", description = "Full random algo")
        boolean fullRandomAlgo = false;

        @Parameter(names = "-badRandomAlgo", description = "The original algo")
        boolean badRandomAlgo = false;

        @Parameter(names = "-bucketAlgo", description = "Bucketing algorithm")
        boolean bucketingAlgo = false;

        @Parameter(names = "-seqAlgo", description = "Sequential algorithm")
        boolean seqAlgo = false;

        @Parameter(names = "-batchSize", description = "batch size")
        int batchSize = 10000;

        @Parameter(names = "-writeBufferSize", description = "write buffer size")
        long writeBufferSize = -1;

        @Parameter(names = "-graphite", description = "graphite server to report to")
        String graphite = null;

    }

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        new JCommander(config, args);

        Configuration hbaseConfig = HBaseConfiguration.create();
        HTable commitHTable = new HTable(hbaseConfig, COMMIT_TABLE_DEFAULT_NAME);
        if (config.writeBufferSize != -1) {
            commitHTable.setWriteBufferSize(config.writeBufferSize);
        }

        final KeyGenerator keygen;
        if (config.fullRandomAlgo) {
            keygen = new FullRandomKeyGenerator();
        } else if (config.badRandomAlgo) {
            keygen = new BadRandomKeyGenerator();
        } else if (config.bucketingAlgo) {
            keygen = new BucketKeyGenerator();
        } else if (config.seqAlgo) {
            keygen = new SeqKeyGenerator();
        } else {
            keygen = null;
            assert (false);
        }
        CommitTable commitTable = new HBaseCommitTable(commitHTable, keygen);
        CommitTable.Writer writer = commitTable.getWriter().get();

        MetricRegistry metrics = new MetricRegistry();
        if (config.graphite != null) {
            String parts[] = config.graphite.split(":");
            String host = parts[0];
            Integer port = Integer.valueOf(parts[1]);

            final Graphite graphite = new Graphite(new InetSocketAddress(host, port));
            final GraphiteReporter reporter = GraphiteReporter.forRegistry(metrics)
                    .prefixedWith("omid-hbase." + keygen.getClass().getSimpleName())
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    .build(graphite);
            reporter.start(10, TimeUnit.SECONDS);
        }
        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(10, TimeUnit.SECONDS);

        Timer flushTimer = metrics.timer("flush");
        Meter commitsMeter = metrics.meter("commits");
        int i = 0;
        long ts = 0;
        while (true) {
            writer.addCommittedTransaction(ts++, ts++);
            if (i++ == config.batchSize) {
                commitsMeter.mark(i);
                long start = System.nanoTime();
                writer.flush().get();
                flushTimer.update((System.nanoTime() - start), TimeUnit.NANOSECONDS);
                i = 0;
            }
        }
    }
}
