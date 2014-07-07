package com.yahoo.omid.transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import com.yahoo.omid.tso.hbase.HBaseTimestampStorage;
import com.yahoo.omid.tsoclient.TSOClient;

public class TestCompaction {

    private static final Logger LOG = LoggerFactory.getLogger(TestCompaction.class);

    private static final String TEST_TABLE = "test-table";
    private static final String TEST_FAMILY = "test-fam";
    private static final String TEST_QUALIFIER = "test-qual";

    private final byte[] table = Bytes.toBytes(TEST_TABLE);
    private final byte[] fam = Bytes.toBytes(TEST_FAMILY);
    private final byte[] qual = Bytes.toBytes(TEST_QUALIFIER);
    private final byte[] data = Bytes.toBytes("testWrite-1");

    private static final int MAX_VERSIONS = 3;

    private Random randomGenerator;
    private AbstractTransactionManager tm;
    private TTable txTable;

    private static Injector injector;

    private static HBaseAdmin admin;
    private static Configuration hbaseConf;
    private static HBaseTestingUtility hbaseTestUtil;
    private static MiniHBaseCluster hbaseCluster;

    private static TSOServer tso;

    private static AggregationClient aggregationClient;

    @BeforeClass
    public static void setupTestCompation() throws Exception {
        injector = Guice.createInjector(
                new TSOForHBaseCompactorTestModule(TSOServerCommandLineConfig.configFactory(1234, 1)));
        hbaseConf = injector.getInstance(Configuration.class);
        setupHBase();
        aggregationClient = new AggregationClient(hbaseConf);
        admin = new HBaseAdmin(hbaseConf);
        createRequiredHBaseTables();
        setupTSO();
    }

    private static void setupHBase() throws Exception {
        LOG.info("********** Setting up HBase **********");
        hbaseTestUtil = new HBaseTestingUtility(hbaseConf);
        LOG.info("********** Creating HBase MiniCluster **********");
        hbaseCluster = hbaseTestUtil.startMiniCluster(3);
    }

    private static void createRequiredHBaseTables() throws IOException {
        createTableIfNotExists(HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME,
                               HBaseTimestampStorage.TSO_FAMILY);
        enableTableIfDisabled(HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME);

        createTableIfNotExists(HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME,
                               HBaseCommitTable.COMMIT_TABLE_FAMILY,
                               HBaseCommitTable.LOW_WATERMARK_FAMILY);
        enableTableIfDisabled(HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME);
    }

    private static void createTableIfNotExists(String tableName, byte[]... families) throws IOException {
        if (!admin.tableExists(tableName)) {
            LOG.info("Creating {} table...", tableName);
            HTableDescriptor desc = new HTableDescriptor(tableName);

            for (byte[] family : families) {
                HColumnDescriptor datafam = new HColumnDescriptor(family);
                datafam.setMaxVersions(MAX_VERSIONS);
                desc.addFamily(datafam);
            }

            desc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
            admin.createTable(desc);
        }
    }

    private static void enableTableIfDisabled(String tableName) throws IOException {
        if (admin.isTableDisabled(tableName)) {
            admin.enableTable(tableName);
            LOG.info("{} enabled...", tableName);
        }
    }

    private static void setupTSO() throws UnknownHostException, IOException, InterruptedException {
        tso = injector.getInstance(TSOServer.class);
        tso.startAndWait();
        TestUtils.waitForSocketListening("localhost", 1234, 100);
        Thread.currentThread().setName("JUnit Thread");
    }

    @AfterClass
    public static void cleanupTestCompation() throws Exception {
        teardownTSO();
        hbaseCluster.shutdown();
    }

    private static void teardownTSO() throws UnknownHostException, IOException, InterruptedException {
        tso.stopAndWait();
        tso = null;
        TestUtils.waitForSocketNotListening("localhost", 1234, 1000);
    }

    @Before
    public void setupTestCompactionIndividualTest() throws Exception {
        createTableIfNotExists(TEST_TABLE, Bytes.toBytes(TEST_FAMILY));
        enableTableIfDisabled(TEST_TABLE);
        randomGenerator = new Random(0xfeedcafeL);
        tm = spy((AbstractTransactionManager) newTransactionManager());
        txTable = new TTable(hbaseConf, TEST_TABLE);
    }

    @After
    public void cleanupTestCompactionIndividualTest() throws Exception {
        admin.disableTable(TEST_TABLE);
        admin.deleteTable(TEST_TABLE);
        txTable.close();
        tm.close();
    }

    private static TransactionManager newTransactionManager() throws Exception {

        BaseConfiguration clientConf = new BaseConfiguration();
        clientConf.setProperty("tso.host", "localhost");
        clientConf.setProperty("tso.port", 1234);

        CommitTable commitTable = injector.getInstance(CommitTable.class);

        TSOClient client = TSOClient.newBuilder().withConfiguration(clientConf).build();

        return HBaseTransactionManager.newBuilder()
                                      .withConfiguration(hbaseConf)
                                      .withCommitTableClient(commitTable.getClient().get())
                                      .withTSOClient(client)
                                      .build();
    }

    @Test
    public void testStandardTXsWithShadowCellsAndWithSTBelowAndAboveLWMArePresevedAfterCompaction() throws Throwable {

        final int ROWS_TO_ADD = 5;

        long rowId = 0L;
        Transaction tx;
        Put put;
        long fakeAssignedLowWatermark = 0L;
        for (int i = 0; i < ROWS_TO_ADD; ++i) {
            rowId = randomGenerator.nextLong();
            tx = tm.begin();
            if (i == (ROWS_TO_ADD / 2)) {
                fakeAssignedLowWatermark = tx.getTransactionId();
                LOG.info("AssignedLowWatermark " + fakeAssignedLowWatermark);
            }
            put = new Put(Bytes.toBytes(rowId));
            put.add(fam, qual, data);
            txTable.put(tx, put);
            tm.commit(tx);
        }

        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);

        // Return a LWM that triggers compaction & stays between 1 and the max start timestamp assigned to previous TXs
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable.Client commitTableClient = spy(omidCompactor.commitTableClient);
        SettableFuture<Long> f = SettableFuture.<Long> create();
        f.set(fakeAssignedLowWatermark);
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClient = commitTableClient;
        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TEST_TABLE);

        LOG.info("Sleeping for 3 secs");
        Thread.currentThread().sleep(3000);
        LOG.info("Waking up after 3 secs");

        // No rows should have been discarded after compacting
        assertEquals("Rows in table after compacting should be " + ROWS_TO_ADD, ROWS_TO_ADD, rowCount(table, fam));
    }

    @Test
    public void testTXWithoutShadowCellsAndWithSTBelowLWMGetsShadowCellHealedAfterCompaction() throws Exception {

        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(tm).updateShadowCells(any(HBaseTransaction.class));

        HBaseTransaction problematicTx = (HBaseTransaction) tm.begin();

        long row = randomGenerator.nextLong();

        // Test shadow cell are created properly
        Put put = new Put(Bytes.toBytes(row));
        put.add(fam, qual, data);
        txTable.put(problematicTx, put);
        try {
            tm.commit(problematicTx);
        } catch (Exception e) { // (*) Crash
            // Do nothing
        }

        assertTrue("Cell should be there",
                HBaseUtils.hasCell(txTable, Bytes.toBytes(row), fam, qual, problematicTx.getStartTimestamp()));
        assertFalse("Shadow cell should not be there",
                HBaseUtils.hasShadowCell(txTable, Bytes.toBytes(row), fam, qual, problematicTx.getStartTimestamp()));

        // Return a LWM that triggers compaction and has all the possible start timestamps below it
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable.Client commitTableClient = spy(omidCompactor.commitTableClient);
        SettableFuture<Long> f = SettableFuture.<Long> create();
        f.set(Long.MAX_VALUE);
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClient = commitTableClient;

        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);

        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TEST_TABLE);

        LOG.info("Sleeping for 3 secs");
        Thread.currentThread().sleep(3000);
        LOG.info("Waking up after 3 secs");

        assertTrue("Cell should be there",
                HBaseUtils.hasCell(txTable, Bytes.toBytes(row), fam, qual, problematicTx.getStartTimestamp()));
        assertTrue("Shadow cell should be there",
                HBaseUtils.hasShadowCell(txTable, Bytes.toBytes(row), fam, qual, problematicTx.getStartTimestamp()));
    }

    @Test
    public void testNeverendingTXsWithSTBelowAndAboveLWMAreDiscardedAndPreservedRespectivelyAfterCompaction()
            throws Throwable {

        // The KV in this transaction should be discarded
        HBaseTransaction neverendingTxBelowLowWatermark = (HBaseTransaction) tm.begin();
        long rowId = randomGenerator.nextLong();
        Put put = new Put(Bytes.toBytes(rowId));
        put.add(fam, qual, data);
        txTable.put(neverendingTxBelowLowWatermark, put);
        assertTrue("Cell should be there",
                HBaseUtils.hasCell(txTable, Bytes.toBytes(rowId), fam, qual,
                        neverendingTxBelowLowWatermark.getStartTimestamp()));
        assertFalse(
                "Shadow cell should not be there",
                HBaseUtils.hasShadowCell(txTable, Bytes.toBytes(rowId), fam, qual,
                        neverendingTxBelowLowWatermark.getStartTimestamp()));

        // The KV in this transaction should be added without the shadow cells
        HBaseTransaction neverendingTxAboveLowWatermark = (HBaseTransaction) tm.begin();
        rowId = randomGenerator.nextLong();
        put = new Put(Bytes.toBytes(rowId));
        put.add(fam, qual, data);
        txTable.put(neverendingTxAboveLowWatermark, put);
        assertTrue(
                "Cell should be there",
                HBaseUtils.hasCell(txTable, Bytes.toBytes(rowId), fam, qual,
                        neverendingTxAboveLowWatermark.getStartTimestamp()));
        assertFalse(
                "Shadow cell should not be there",
                HBaseUtils.hasShadowCell(txTable, Bytes.toBytes(rowId), fam, qual,
                        neverendingTxAboveLowWatermark.getStartTimestamp()));

        assertEquals("Rows in table before flushing should be 2", 2, rowCount(table, fam));
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);
        assertEquals("Rows in table after flushing should be 2", 2, rowCount(table, fam));

        // Return a LWM that triggers compaction and stays between both ST of TXs, so assignt 1st TX's start timestamp
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable.Client commitTableClient = spy(omidCompactor.commitTableClient);
        SettableFuture<Long> f = SettableFuture.<Long> create();
        f.set(neverendingTxBelowLowWatermark.getStartTimestamp());
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClient = commitTableClient;
        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TEST_TABLE);

        LOG.info("Sleeping for 3 secs");
        Thread.currentThread().sleep(3000);
        LOG.info("Waking up after 3 secs");

        // One row should have been discarded after compacting
        assertEquals("There should be only one row in table after compacting", 1, rowCount(table, fam));
    }

    @Test
    public void testRowsUnalteredWhenCommitTableCannotBeReached() throws Throwable {

        // The KV in this transaction should be discarded but in the end should remain there because
        // the commit table won't be accessed (simulating an error on access)
        HBaseTransaction neverendingTx = (HBaseTransaction) tm.begin();
        long rowId = randomGenerator.nextLong();
        Put put = new Put(Bytes.toBytes(rowId));
        put.add(fam, qual, data);
        txTable.put(neverendingTx, put);
        assertTrue("Cell should be there",
                HBaseUtils.hasCell(txTable, Bytes.toBytes(rowId), fam, qual, neverendingTx.getStartTimestamp()));
        assertFalse("Shadow cell should not be there",
                HBaseUtils.hasShadowCell(txTable, Bytes.toBytes(rowId), fam, qual,
                        neverendingTx.getStartTimestamp()));

        assertEquals("There should be only one rows in table before flushing", 1, rowCount(table, fam));
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);
        assertEquals("There should be only one rows in table after flushing", 1, rowCount(table, fam));

        // Break access to CommitTable functionality in Compactor
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable.Client commitTableClient = spy(omidCompactor.commitTableClient);
        SettableFuture<Long> f = SettableFuture.<Long> create();
        f.setException(new IOException("Unable to read"));
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClient = commitTableClient;

        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TEST_TABLE); // Should trigger the error when accessing CommitTable funct.

        LOG.info("Sleeping for 3 secs");
        Thread.currentThread().sleep(3000);
        LOG.info("Waking up after 3 secs");

        // All rows should be there after the failed compaction
        assertEquals("There should be only one row in table after compacting", 1, rowCount(table, fam));
    }

    @Test
    public void testOriginalTableParametersAreAvoidedAlsoWhenCompacting() throws Throwable {

        Transaction tx;
        Put put;
        long rowId = randomGenerator.nextLong();
        for (int versionCount = 0; versionCount <= (2 * MAX_VERSIONS); versionCount++) {
            tx = tm.begin();
            put = new Put(Bytes.toBytes(rowId));
            put.add(fam, qual, Bytes.toBytes("testWrite-" + versionCount));
            txTable.put(tx, put);
            tm.commit(tx);
        }

        tx = tm.begin();
        Get get = new Get(Bytes.toBytes(rowId));
        get.setMaxVersions(2 * MAX_VERSIONS);
        assertEquals("Max versions should be set to " + (2 * MAX_VERSIONS), (2 * MAX_VERSIONS), get.getMaxVersions());
        get.addColumn(fam, qual);
        Result result = txTable.get(tx, get);
        tm.commit(tx);
        List<KeyValue> column = result.getColumn(fam, qual);
        assertEquals("There should be only one version in the result", 1, column.size());

        assertEquals("There should be only one row in table before flushing", 1, rowCount(table, fam));
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);
        assertEquals("There should be only one row in table after flushing", 1, rowCount(table, fam));

        // Return a LWM that triggers compaction
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable.Client commitTableClient = spy(omidCompactor.commitTableClient);
        SettableFuture<Long> f = SettableFuture.<Long> create();
        f.set(Long.MAX_VALUE);
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClient = commitTableClient;
        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TEST_TABLE);

        LOG.info("Sleeping for 3 secs");
        Thread.currentThread().sleep(3000);
        LOG.info("Waking up after 3 secs");

        // One row should have been discarded after compacting
        assertEquals("There should be only one row in table after compacting", 1, rowCount(table, fam));

        tx = tm.begin();
        get = new Get(Bytes.toBytes(rowId));
        get.setMaxVersions(2 * MAX_VERSIONS);
        assertEquals("Max versions should be set to " + (2 * MAX_VERSIONS), (2 * MAX_VERSIONS), get.getMaxVersions());
        get.addColumn(fam, qual);
        result = txTable.get(tx, get);
        tm.commit(tx);
        column = result.getColumn(fam, qual);
        assertEquals("There should be only one version in the result", 1, column.size());
        assertEquals("Values don't match", "testWrite-" + (2 * MAX_VERSIONS), Bytes.toString(column.get(0).getValue()));
    }

    private static long rowCount(byte[] table, byte[] family) throws Throwable {
        Scan scan = new Scan();
        scan.addFamily(family);
        return aggregationClient.rowCount(table, null, scan);
    }

}