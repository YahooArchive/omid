package com.yahoo.omid.committable.hbase;

import static com.yahoo.omid.committable.hbase.HBaseCommitTable.COMMIT_TABLE_FAMILY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import org.apache.bookkeeper.client.BKException.BKLedgerFencedException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerSet;
import org.apache.bookkeeper.client.LedgerSet.Entry;
import org.apache.bookkeeper.client.LedgerSet.MetadataStorage;
import org.apache.bookkeeper.client.LedgerSet.ReaderWriter;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.omid.committable.CommitTable.Writer;
import com.yahoo.omid.committable.hbase.HBaseHACommitTable.HBaseLogPositionReader;
import com.yahoo.omid.committable.hbase.ZKBasedMetadataStorage.ZookeeperVersion;

public class TestHAHBaseCommitTable {

    public static class BKCluster extends BookKeeperClusterTestCase {

        public BKCluster(int numBookies) {
            super(numBookies);
        }

        public String getZKCluster() {
            return this.zkUtil.getZooKeeperConnectString();
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(TestHAHBaseCommitTable.class);

    private static final int NUM_BOOKIES = 3;

    private static final String TEST_TABLE = "TEST";

    private static final TableName TABLE_NAME = TableName.valueOf(TEST_TABLE);

    private static HBaseTestingUtility testutil;
    private static MiniHBaseCluster hbasecluster;
    protected static Configuration hbaseConf;
    private static AggregationClient aggregationClient;

    private static BKCluster bkCluster;

    @BeforeClass
    public static void setUpClass() throws Exception {

        // HBase setup
        hbaseConf = HBaseConfiguration.create();

        LOG.info("Create hbase");
        testutil = new HBaseTestingUtility(hbaseConf);
        hbasecluster = testutil.startMiniCluster(1);
        aggregationClient = new AggregationClient(hbaseConf);

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (hbasecluster != null) {
            testutil.shutdownMiniCluster();
        }
    }

    @BeforeMethod
    public void setUp() throws Exception {
        // Fresh BK setup
        bkCluster = new BKCluster(NUM_BOOKIES);
        bkCluster.setUp();
        LOG.info("Fresh BK Cluster started");

        HBaseAdmin admin = testutil.getHBaseAdmin();

        // Configure test table
        if (!admin.tableExists(TEST_TABLE)) {
            HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);

            HColumnDescriptor datafam = new HColumnDescriptor(HBaseCommitTable.COMMIT_TABLE_FAMILY);
            datafam.setMaxVersions(1);
            desc.addFamily(datafam);

            desc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
            admin.createTable(desc);

            LOG.info("Table {} created", TEST_TABLE);
        }

        if (admin.isTableDisabled(TEST_TABLE)) { // Defensive
            admin.enableTable(TEST_TABLE);

            LOG.info("Table {} enabled", TEST_TABLE);
        }

        LOG.info("TABLES AVAILABLE IN TEST:");
        HTableDescriptor[] tables = admin.listTables();
        for (HTableDescriptor t : tables) {
            LOG.info("\t{}", t.getNameAsString());
        }
    }

    @AfterMethod
    public void tearDown() {
        try (HBaseAdmin admin = testutil.getHBaseAdmin()) {
            // Delete tables used
            admin.disableTable(TEST_TABLE);
            admin.deleteTable(TEST_TABLE);
            LOG.info("{} table deleted after test");

            // Shutdown BK after each method
            bkCluster.tearDown();
            LOG.info("BK Cluster shutdown");
        } catch (Exception e) {
            LOG.error("Error tearing down", e);
        }
    }


    /**
     * Check that when we write through the HBaseHAWriter, we end up
     * having the results in both, the BK Log and HBase
     */
    @Test
    public void testBasicFunctionality() throws Throwable {
        HBaseCommitTableConfig config = new HBaseCommitTableConfig();
        config.setTableName(TEST_TABLE);
        config.enableHA(true);

        String zkCluster = bkCluster.getZKCluster();

        BookKeeper bk = HBaseHACommitTable.provideBookKeeperClient(zkCluster);

        MetadataStorage metadataStorage = new ZKBasedMetadataStorage(
                HBaseHACommitTable.provideZookeeperClient(zkCluster));

        // Create the HA commit table and get the HA writer to test
        HBaseHACommitTable commitTable = new HBaseHACommitTable(hbaseConf, config, bk, metadataStorage);
        Writer writer = commitTable.getWriter().get();

        // Test that the first time the table is empty
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 0, "Rows should be 0!");

        // Write 100 transactions
        for (int i = 0; i < 100; i++) {
            writer.addCommittedTransaction(i, i + 1);
        }

        // Test that the table is empty before flushing to HBase
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 0, "Rows should be 0!");

        writer.flush().get();

        // Test that the results are in HBase...
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 100, "Rows should be 100!");
        // ... in Metadata Storage there's an initial version...
        Versioned<byte[]> initialLogEntry = metadataStorage.read(HBaseHACommitTable.COMMIT_CACHE_BK_LEDGERSET_NAME)
                .get();
        ZookeeperVersion initialZKVersion = (ZookeeperVersion) initialLogEntry.getVersion();
        assertEquals(initialZKVersion.getVersion(), 0);
        // ... and there's still no data about log last entry in HBase...
        HBaseLogPositionReader logPosReader = commitTable.new HBaseLogPositionReader();
        assertFalse(logPosReader.readLogPosition().isPresent());

        // Write 100 more transactions and flush
        for (int i = 100; i < 2 * 100; i++) {
            writer.addCommittedTransaction(i, i + 1);
        }
        writer.flush().get();

        // Test that the results are in HBase...
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), (2 * 100) + 1,
                "Rows should be 200 (+1 Row of the last entry id in log)!");
        // ... and there's data about log last entry in HBase...
        assertTrue(logPosReader.readLogPosition().isPresent());
        // ... and what is in the log is the 2 previous entries written (one per set of txs)
        ReaderWriter bkReaderWriter = getBKLogReaderWriter(bk, metadataStorage);
        int entriesInLogCount = 0;
        while (bkReaderWriter.hasMoreEntries().get()) {
            Entry entry = bkReaderWriter.nextEntry().get();
            ByteBuffer bb = ByteBuffer.wrap(entry.getBytes());
            int numberOfTxBoundaries = bb.getInt();
            assertEquals(numberOfTxBoundaries, 100);
            LOG.info("New log entry found with {} TxBoundaries", numberOfTxBoundaries);
            for (int i = 0; i < numberOfTxBoundaries; i++) {
                long startTimestamp = bb.getLong();
                long commitTimestamp = bb.getLong();
                assertEquals(startTimestamp, (100 * entriesInLogCount) + i);
                assertEquals(commitTimestamp, (100 * entriesInLogCount) + (i + 1));
            }
            entriesInLogCount++;
        }
        assertEquals(entriesInLogCount, 2);

        // Close resources
        logPosReader.close();
        writer.close();
    }

    /**
     * Check that when we write through the HBaseHAWriter flushing
     * multiple times, we get the correct results in the log and in
     * HBase
     */
    @Test
    public void testMultiFlush() throws Throwable {
        HBaseCommitTableConfig config = new HBaseCommitTableConfig();
        config.setTableName(TEST_TABLE);
        config.enableHA(true);

        String zkCluster = bkCluster.getZKCluster();

        BookKeeper bk = HBaseHACommitTable.provideBookKeeperClient(zkCluster);

        MetadataStorage metadataStorage = new ZKBasedMetadataStorage(
                HBaseHACommitTable.provideZookeeperClient(zkCluster));

        // Create the HA commit table and get the HA writer to test
        HBaseHACommitTable commitTable = new HBaseHACommitTable(hbaseConf, config, bk, metadataStorage);
        Writer writer = commitTable.getWriter().get();

        // Test that the first time the table is empty
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 0, "Rows should be 0!");

        // Test multiple flushes without writing anything
        writer.flush().get();
        writer.flush().get();
        writer.flush().get();

        // Test the table is still empty...
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 0, "Rows should be 0!");

        // ... in Metadata Storage there's no initial version...
        Versioned<byte[]> initialLogEntry;
        try {
            initialLogEntry = metadataStorage.read(HBaseHACommitTable.COMMIT_CACHE_BK_LEDGERSET_NAME).get();
        } catch (ExecutionException e) {
            // Expected
            assertEquals(e.getCause().getClass(), MetadataStorage.NoKeyException.class);
        }
        // ... and there's still no data about log last entry in HBase...
        HBaseLogPositionReader logPosReader = commitTable.new HBaseLogPositionReader();
        assertFalse(logPosReader.readLogPosition().isPresent());

        // Write 100 transactions...
        for (int i = 0; i < 100; i++) {
            writer.addCommittedTransaction(i, i + 1);
        }
        // ...test the table is still empty...
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 0, "Rows should be 0!");
        // ... then flush
        writer.flush().get();
        // ...test the table has 100 elements...
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 100, "Rows should be 100!");
        // ... and in Metadata Storage there's an initial version...
        initialLogEntry = metadataStorage.read(HBaseHACommitTable.COMMIT_CACHE_BK_LEDGERSET_NAME)
                .get();
        ZookeeperVersion initialZKVersion = (ZookeeperVersion) initialLogEntry.getVersion();
        assertEquals(initialZKVersion.getVersion(), 0);
        // ... and there's still no data about log last entry in HBase...
        assertFalse(logPosReader.readLogPosition().isPresent());

        // Test multiple flushes without writing anything and check
        // that still get the same results
        writer.flush().get();
        writer.flush().get();
        writer.flush().get();

        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 100, "Rows should be 100!");
        initialLogEntry = metadataStorage.read(HBaseHACommitTable.COMMIT_CACHE_BK_LEDGERSET_NAME)
                .get();
        initialZKVersion = (ZookeeperVersion) initialLogEntry.getVersion();
        assertEquals(initialZKVersion.getVersion(), 0);
        assertFalse(logPosReader.readLogPosition().isPresent());

        // Close resources
        logPosReader.close();
        writer.close();
    }

    /**
     * Create an initial instance of a commit table writer and check that it
     * can write commit table entries to both the Log and HBase. Then,
     * create a new writer instance and check how the first writer instance
     * is fenced and can not write to the log
     */
    @Test
    public void testFencing() throws Throwable {
        HBaseCommitTableConfig config = new HBaseCommitTableConfig();
        config.setTableName(TEST_TABLE);
        config.enableHA(true);

        String zkCluster = bkCluster.getZKCluster();

        BookKeeper bk = HBaseHACommitTable.provideBookKeeperClient(zkCluster);

        // Create an instance of an HA commit table to test and the writer that will be fenced
        MetadataStorage metadataStorage1 = new ZKBasedMetadataStorage(
                HBaseHACommitTable.provideZookeeperClient(zkCluster));
        HBaseHACommitTable commitTable1 = new HBaseHACommitTable(hbaseConf, config, bk, metadataStorage1);
        Writer fencedWriter = commitTable1.getWriter().get();

        // Test that the first time the table is empty
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 0, "Rows should be 0!");

        // Write 100 transactions
        for (int i = 0; i < 100; i++) {
            fencedWriter.addCommittedTransaction(i, i + 1);
        }

        // The writer should not be fenced yet, so test that the results
        // are in HBase (and therefore also in the log)
        fencedWriter.flush().get();
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 100, "Rows should be 100!");

        // Create another writer instance that will fence the previous HA writer...
        Writer writer = commitTable1.getWriter().get();

        // Check fencing. The first writer should not be allowed to write anymore to the log
        fencedWriter.addCommittedTransaction(1000, 1000 + 1);
        try {
            fencedWriter.flush().get();
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getCause().getClass(), BKLedgerFencedException.class);
        }

        // Test that the commit table state still has the same results
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 100, "Rows should be 100!");

        // Close resources
        fencedWriter.close();
        writer.close();
    }

    /**
     * Create an instance of a commit table with an anomalous writer that
     * emulates abnormal behavior (can write commit table entries to the
     * log but not to HBase.) Then create a new commit table instance with
     * a normal writer, and check that the commit table is repaired by with
     * the contents of the log. Fencing of the previous writer is checked
     * again, just in case.
     */
    @Test
    public void testRollForwardCommitCacheFromLog() throws Throwable {
        HBaseCommitTableConfig config = new HBaseCommitTableConfig();
        config.setTableName(TEST_TABLE);
        config.enableHA(true);

        String zkCluster = bkCluster.getZKCluster();

        BookKeeper bk = HBaseHACommitTable.provideBookKeeperClient(zkCluster);

        MetadataStorage metadataStorage1 = new ZKBasedMetadataStorage(
                HBaseHACommitTable.provideZookeeperClient(zkCluster));

        // Create the HA commit table to test with an anomalous writer
        AnomalousHBaseHACommitTable commitTable1 = new AnomalousHBaseHACommitTable(hbaseConf,
                                                                                   config,
                                                                                   bk,
                                                                                   metadataStorage1);

        // Create an anomalous writer :-)
        Writer anomalousWriter = commitTable1.new HBaseHAWriter(
                commitTable1.new AnomalousHBaseWriter(hbaseConf, TEST_TABLE));

        // Test that the first time the table is empty
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 0, "Rows should be 0!");

        // Write 100 transactions
        for (int i = 0; i < 100; i++) {
            anomalousWriter.addCommittedTransaction(i, i + 1);
        }

        anomalousWriter.flush().get();

        // Test that the results are NOT in HBase...
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 0, "Rows should be 0!");

        // Create a normal HA writer
        MetadataStorage metadataStorage2 = new ZKBasedMetadataStorage(
                HBaseHACommitTable.provideZookeeperClient(zkCluster));

        // Create the HA commit table to test with a normal HA writer...
        HBaseHACommitTable commitTable2 = new HBaseHACommitTable(hbaseConf, config, bk, metadataStorage2);
        // When creating the HA writer, the commit table state should be reconciled
        Writer writer = commitTable2.getWriter().get();

        // Test that the commit table state has the previous transactions found in the log
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 100, "Rows should be 100!");

        // Check that the ZK version is still the original one
        Versioned<byte[]> logEntry = metadataStorage2
                .read(HBaseHACommitTable.COMMIT_CACHE_BK_LEDGERSET_NAME)
                .get();
        ZookeeperVersion initialZKVersion = (ZookeeperVersion) logEntry.getVersion();
        assertEquals(initialZKVersion.getVersion(), 0);

        // Check fencing. The anomalous writer should not be allowed to write anymore to the log
        anomalousWriter.addCommittedTransaction(1000, 1000 + 1);
        try {
            anomalousWriter.flush().get();
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getCause().getClass(), BKLedgerFencedException.class);
        }

        // Write 100 more transactions this time with the normal writer and flush
        for (int i = 100; i < 2 * 100; i++) {
            writer.addCommittedTransaction(i, i + 1);
        }
        writer.flush().get();

        // Check that the ZK version has been increased after writing
        logEntry = metadataStorage2
                .read(HBaseHACommitTable.COMMIT_CACHE_BK_LEDGERSET_NAME)
                .get();
        ZookeeperVersion zkVersion = (ZookeeperVersion) logEntry.getVersion();
        assertEquals(zkVersion.getVersion(), 1);

        // Test that the results are in HBase...
        assertEquals(rowCount(TABLE_NAME, COMMIT_TABLE_FAMILY), 2 * 100, "Rows should be 200!");

        // Close resources
        anomalousWriter.close();
        writer.close();
    }

    // **************************** Helper methods ****************************

    private static ReaderWriter getBKLogReaderWriter(BookKeeper bk, MetadataStorage metadataStore) throws IOException {

        try {
            return LedgerSet.newBuilder()
                            .setClient(bk)
                            .setMetadataStorage(metadataStore)
                            .buildReaderWriter(HBaseHACommitTable.COMMIT_CACHE_BK_LEDGERSET_NAME).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted getting the ledger set reader/writer");
        } catch (ExecutionException e) {
            throw new IOException("Error getting the ledger set reader/writer", e.getCause());
        }

    }

    private static long rowCount(TableName table, byte[] family) throws Throwable {
        Scan scan = new Scan();
        scan.addFamily(family);
        return aggregationClient.rowCount(table, new LongColumnInterpreter(), scan);
    }

}
