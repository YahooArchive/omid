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

    private final byte[] fam = Bytes.toBytes(TEST_FAMILY);
    private final byte[] qual = Bytes.toBytes(TEST_QUALIFIER);
    private final byte[] data = Bytes.toBytes("testWrite-1");

    private static final int BUCKET_SIZE = 32;
    private static final int MAX_VERSIONS = 3;

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
    }

    @After
    public void cleanupTestCompactionIndividualTest() throws Exception {
        admin.disableTable(TEST_TABLE);
        admin.deleteTable(TEST_TABLE);
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
    public void testShadowCellIsHealedAfterCompaction() throws Exception {

        Random randomGenerator = new Random(0xfeedcafeL);

        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager());
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(tm).updateShadowCells(any(HBaseTransaction.class));

        TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction problematicTx = (HBaseTransaction) tm.begin();

        long row = randomGenerator.nextLong();

        // Test shadow cell are created properly
        Put put = new Put(Bytes.toBytes(row));
        put.add(fam, qual, data);
        table.put(problematicTx, put);
        try {
            tm.commit(problematicTx);
        } catch (Exception e) { // (*) Crash
            // Do nothing
        }

        assertTrue("Cell should be there",
                TestShadowCells.hasCell(table, Bytes.toBytes(row), fam, qual, problematicTx.getStartTimestamp()));
        assertFalse("Shadow cell should not be there",
                TestShadowCells.hasShadowCell(table, Bytes.toBytes(row), fam, qual, problematicTx.getStartTimestamp()));

        // Return a LWM that triggers compaction
        LOG.info("Regions in table {}: {}", TEST_TABLE, hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).size());
        OmidCompactor omidCompactor = (OmidCompactor) hbaseCluster.getRegions(Bytes.toBytes(TEST_TABLE)).get(0)
                .getCoprocessorHost().findCoprocessor(OmidCompactor.class.getName());
        CommitTable.Client commitTableClient = spy(omidCompactor.commitTableClient);
        SettableFuture<Long> f = SettableFuture.<Long> create();
        f.set(10L);
        doReturn(f).when(commitTableClient).readLowWatermark();
        omidCompactor.commitTableClient = commitTableClient;

        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);

        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TEST_TABLE);

        LOG.info("Sleeping for 10 secs");
        Thread.currentThread().sleep(10000);
        LOG.info("Waking up after 10 secs");

        assertTrue("Cell should be there",
                TestShadowCells.hasCell(table, Bytes.toBytes(row), fam, qual, problematicTx.getStartTimestamp()));
        assertTrue("Shadow cell should be there",
                TestShadowCells.hasShadowCell(table, Bytes.toBytes(row), fam, qual, problematicTx.getStartTimestamp()));
    }

    @Test
    public void testCompactionCoprocessorBasics() throws Throwable {

        byte[] tableName = Bytes.toBytes(TEST_TABLE);

        Random randomGenerator = new Random(0xfeedcafeL);

        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");

        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager());
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Put put;
        long rowId = 0L;

        // The KV in this transaction should find the shadow cells in the commit table
        HBaseTransaction problematicTx = (HBaseTransaction) tm.begin();
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(tm).updateShadowCells(problematicTx);
        rowId = randomGenerator.nextLong();
        put = new Put(Bytes.toBytes(rowId));
        put.add(fam, col, data1);
        // put.add(fam, col2, data1);
        tt.put(problematicTx, put);
        try {
            tm.commit(problematicTx);
        } catch (Exception e) { // (*) crash
            // Do nothing
        }

        assertTrue("Cell should be there",
                TestShadowCells.hasCell(tt, Bytes.toBytes(rowId), fam, col, problematicTx.getStartTimestamp()));
        assertFalse("Shadow cell should not be there",
                TestShadowCells.hasShadowCell(tt, Bytes.toBytes(rowId), fam, col, problematicTx.getStartTimestamp()));

        // The KV in this transaction should be discarded
        HBaseTransaction neverendingTx = (HBaseTransaction) tm.begin();
        rowId = randomGenerator.nextLong();
        put = new Put(Bytes.toBytes(rowId));
        put.add(fam, col, data1);
        tt.put(neverendingTx, put);
        assertTrue("Cell should be there",
                TestShadowCells.hasCell(tt, Bytes.toBytes(rowId), fam, col, neverendingTx.getStartTimestamp()));
        assertFalse("Shadow cell should not be there",
                TestShadowCells.hasShadowCell(tt, Bytes.toBytes(rowId), fam, col, neverendingTx.getStartTimestamp()));

        Transaction tx;
        LOG.info("Filling single bucket in table {}", TEST_TABLE);
        for (int i = 0; i < (BUCKET_SIZE + 1); ++i) {
            rowId = randomGenerator.nextLong();
            tx = tm.begin();
            put = new Put(Bytes.toBytes(rowId));
            put.add(fam, col, data1);
            tt.put(tx, put);
            tm.commit(tx);
        }

        // The KV in this transaction should be added without the shadow cells
        HBaseTransaction neverendingTxAboveLowWatermark = (HBaseTransaction) tm.begin();
        rowId = randomGenerator.nextLong();
        put = new Put(Bytes.toBytes(rowId));
        put.add(fam, col, data1);
        tt.put(neverendingTxAboveLowWatermark, put);
        assertTrue("Cell should be there",
                TestShadowCells.hasCell(
                        tt, Bytes.toBytes(rowId), fam, col, neverendingTxAboveLowWatermark.getStartTimestamp()));
        assertFalse("Shadow cell should not be there",
                TestShadowCells.hasShadowCell(
                        tt, Bytes.toBytes(rowId), fam, col, neverendingTxAboveLowWatermark.getStartTimestamp()));

        assertEquals("Rows in table before flushing should be " + (BUCKET_SIZE + 4), (BUCKET_SIZE + 4),
                rowCount(tableName, fam));
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);
        assertEquals("Rows in table after flushing should be " + (BUCKET_SIZE + 4), (BUCKET_SIZE + 4),
                rowCount(tableName, fam));

        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TEST_TABLE);

        LOG.info("Sleeping for 10 secs");
        Thread.currentThread().sleep(10000);
        LOG.info("Waking up after 10 secs");

        // One row should have been discarded after compacting
        assertEquals("Rows in table after compacting should be " + (BUCKET_SIZE + 3), (BUCKET_SIZE + 3),
                rowCount(tableName, fam));

    }

    @Test
    public void testRowsUnalteredWhenCommitTableCannotBeReached() throws Throwable {
        byte[] tableName = Bytes.toBytes(TEST_TABLE);

        Random randomGenerator = new Random(0xfeedcafeL);

        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");

        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager());
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Put put;
        long rowId = 0L;

        Transaction tx;

        // The KV in this transaction should be discarded
        HBaseTransaction neverendingTx = (HBaseTransaction) tm.begin();
        rowId = randomGenerator.nextLong();
        put = new Put(Bytes.toBytes(rowId));
        put.add(fam, col, data1);
        tt.put(neverendingTx, put);
        assertTrue("Cell should be there",
                TestShadowCells.hasCell(tt, Bytes.toBytes(rowId), fam, col, neverendingTx.getStartTimestamp()));
        assertFalse("Shadow cell should not be there",
                TestShadowCells.hasShadowCell(tt, Bytes.toBytes(rowId), fam, col, neverendingTx.getStartTimestamp()));

        LOG.info("Filling single bucket in table {}", TEST_TABLE);
        for (int i = 0; i < (BUCKET_SIZE + 1); ++i) {
            rowId = randomGenerator.nextLong();
            tx = tm.begin();
            put = new Put(Bytes.toBytes(rowId));
            put.add(fam, col, data1);
            tt.put(tx, put);
            tm.commit(tx);
        }

        assertEquals("Rows in table before flushing should be " + (BUCKET_SIZE + 2), (BUCKET_SIZE + 2),
                rowCount(tableName, fam));
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);
        assertEquals("Rows in table after flushing should be " + (BUCKET_SIZE + 2), (BUCKET_SIZE + 2),
                rowCount(tableName, fam));

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

        LOG.info("Sleeping for 10 secs");
        Thread.currentThread().sleep(10000);
        LOG.info("Waking up after 10 secs");

        // All rows should be there after the failed compaction
        assertEquals("Rows in table after compacting should be " + (BUCKET_SIZE + 2), (BUCKET_SIZE + 2),
                rowCount(tableName, fam));
    }

    @Test
    public void testOriginalTableParametersAreAvoided() throws Throwable {
        byte[] tableName = Bytes.toBytes(TEST_TABLE);

        Random randomGenerator = new Random(0xfeedcafeL);

        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data = Bytes.toBytes("testWriteXXX");

        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager());
        TTable tt = new TTable(hbaseConf, TEST_TABLE);

        Put put;
        long rowId = 0L;

        Transaction tx;

        // The KV in this transaction should be discarded
        HBaseTransaction neverendingTx = (HBaseTransaction) tm.begin();
        rowId = randomGenerator.nextLong();
        put = new Put(Bytes.toBytes(rowId));
        put.add(fam, col, data);
        tt.put(neverendingTx, put);
        assertTrue("Cell should be there",
                TestShadowCells.hasCell(tt, Bytes.toBytes(rowId), fam, col, neverendingTx.getStartTimestamp()));
        assertFalse("Shadow cell should not be there",
                TestShadowCells.hasShadowCell(tt, Bytes.toBytes(rowId), fam, col, neverendingTx.getStartTimestamp()));

        LOG.info("Filling single bucket in table {}", TEST_TABLE);
        for (int i = 0; i < (BUCKET_SIZE + 1); ++i) {
            rowId = randomGenerator.nextLong();
            for (int versionCount = 0; versionCount <= MAX_VERSIONS; versionCount++) {
                tx = tm.begin();
                put = new Put(Bytes.toBytes(rowId));
                put.add(fam, col, Bytes.toBytes("testWrite-" + versionCount));
                tt.put(tx, put);
                tm.commit(tx);
            }
        }

        tx = tm.begin();
        Get get = new Get(Bytes.toBytes(rowId));
        get.setMaxVersions(MAX_VERSIONS);
        assertEquals("Max versions should be set to " + MAX_VERSIONS, MAX_VERSIONS, get.getMaxVersions());
        get.addColumn(fam, col);
        Result result = tt.get(tx, get);
        tm.commit(tx);
        List<KeyValue> column = result.getColumn(fam, col);
        assertEquals("There should be only one version in the result", 1, column.size());

        assertEquals("Rows in table before flushing should be " + (BUCKET_SIZE + 2), (BUCKET_SIZE + 2),
                rowCount(tableName, fam));
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);
        assertEquals("Rows in table after flushing should be " + (BUCKET_SIZE + 2), (BUCKET_SIZE + 2),
                rowCount(tableName, fam));

        LOG.info("Compacting table {}", TEST_TABLE);
        admin.majorCompact(TEST_TABLE);

        LOG.info("Sleeping for 10 secs");
        Thread.currentThread().sleep(10000);
        LOG.info("Waking up after 10 secs");

        // One row should have been discarded after compacting
        assertEquals("Rows in table after compacting should be " + (BUCKET_SIZE + 1), (BUCKET_SIZE + 1),
                rowCount(tableName, fam));

        tx = tm.begin();
        get = new Get(Bytes.toBytes(rowId));
        get.setMaxVersions(MAX_VERSIONS);
        assertEquals("Max versions should be set to " + MAX_VERSIONS, MAX_VERSIONS, get.getMaxVersions());
        get.addColumn(fam, col);
        result = tt.get(tx, get);
        tm.commit(tx);
        column = result.getColumn(fam, col);
        assertEquals("There should be only one version in the result", 1, column.size());
        assertEquals("Values don't match", "testWrite-" + MAX_VERSIONS, Bytes.toString(column.get(0).getValue()));

    }

    private static long rowCount(byte[] table, byte[] family) throws Throwable {
        Scan scan = new Scan();
        scan.addFamily(family);
        return aggregationClient.rowCount(table, null, scan);
    }
}