package com.yahoo.omid.transaction;

import static com.yahoo.omid.tso.hbase.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.tso.hbase.HBaseTimestampStorage.TSO_FAMILY;
import static com.yahoo.omid.committable.hbase.HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.committable.hbase.HBaseCommitTable.COMMIT_TABLE_FAMILY;
import static com.yahoo.omid.committable.hbase.HBaseCommitTable.LOW_WATERMARK_FAMILY;
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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
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
import com.yahoo.omid.transaction.HBaseTransaction;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import com.yahoo.omid.tsoclient.TSOClient;

public class TestCompaction {

    private static final Logger LOG = LoggerFactory.getLogger(TestCompaction.class);

    private static final String TEST_TABLE = "test-table";
    private static final String TEST_FAMILY = "test-fam";
    private static final String TEST_QUALIFIER = "test-qual";

    private final byte[] fam = Bytes.toBytes(TEST_FAMILY);
    private final byte[] qual = Bytes.toBytes(TEST_QUALIFIER);
    private final byte[] data = Bytes.toBytes("testWrite-1");

    public static final TableName TABLE_NAME = TableName.valueOf(TEST_TABLE);

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
        createTableIfNotExists(TableName.valueOf(TIMESTAMP_TABLE_DEFAULT_NAME),
                               TSO_FAMILY);
        enableTableIfDisabled(TIMESTAMP_TABLE_DEFAULT_NAME);

        createTableIfNotExists(TableName.valueOf(COMMIT_TABLE_DEFAULT_NAME),
                               COMMIT_TABLE_FAMILY,
                               LOW_WATERMARK_FAMILY);
        enableTableIfDisabled(COMMIT_TABLE_DEFAULT_NAME);
    }

    private static void createTableIfNotExists(TableName tableName, byte[]... families) throws IOException {
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
            for (byte[] family : families) {
                CompactorUtil.enableOmidCompaction(hbaseConf, tableName, family);
            }
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
        createTableIfNotExists(TABLE_NAME, Bytes.toBytes(TEST_FAMILY));
        assertTrue("Table " + TEST_TABLE + " should exist", admin.tableExists(TABLE_NAME));
        enableTableIfDisabled(TEST_TABLE);
        assertFalse("Table " + TEST_TABLE + " should be enabled", admin.isTableDisabled(TABLE_NAME));
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

        long fakeAssignedLowWatermark = 0L;
        for (int i = 0; i < ROWS_TO_ADD; ++i) {
            long rowId = randomGenerator.nextLong();
            Transaction tx = tm.begin();
            if (i == (ROWS_TO_ADD / 2)) {
                fakeAssignedLowWatermark = tx.getTransactionId();
                LOG.info("AssignedLowWatermark " + fakeAssignedLowWatermark);
            }
            Put put = new Put(Bytes.toBytes(rowId));
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
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");

        // No rows should have been discarded after compacting
        assertEquals("Rows in table after compacting should be " + ROWS_TO_ADD, ROWS_TO_ADD, rowCount(TABLE_NAME, fam));
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
                HBaseUtils.hasCell(Bytes.toBytes(row),
                                   fam,
                                   qual,
                                   problematicTx.getStartTimestamp(),
                                   new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                HBaseUtils.hasShadowCell(Bytes.toBytes(row),
                                         fam,
                                         qual,
                                         problematicTx.getStartTimestamp(),
                                         new TTableCellGetterAdapter(txTable)));

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
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");

        assertTrue("Cell should be there",
                HBaseUtils.hasCell(Bytes.toBytes(row),
                                   fam,
                                   qual,
                                   problematicTx.getStartTimestamp(),
                                   new TTableCellGetterAdapter(txTable)));
        assertTrue("Shadow cell should not be there",
                HBaseUtils.hasShadowCell(Bytes.toBytes(row),
                                         fam,
                                         qual,
                                         problematicTx.getStartTimestamp(),
                                         new TTableCellGetterAdapter(txTable)));
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
                HBaseUtils.hasCell(Bytes.toBytes(rowId),
                                   fam,
                                   qual,
                                   neverendingTxBelowLowWatermark.getStartTimestamp(),
                                   new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                HBaseUtils.hasShadowCell(Bytes.toBytes(rowId),
                                         fam,
                                         qual,
                                         neverendingTxBelowLowWatermark.getStartTimestamp(),
                                         new TTableCellGetterAdapter(txTable)));

        // The KV in this transaction should be added without the shadow cells
        HBaseTransaction neverendingTxAboveLowWatermark = (HBaseTransaction) tm.begin();
        long anotherRowId = randomGenerator.nextLong();
        put = new Put(Bytes.toBytes(anotherRowId));
        put.add(fam, qual, data);
        txTable.put(neverendingTxAboveLowWatermark, put);
        assertTrue("Cell should be there",
                HBaseUtils.hasCell(Bytes.toBytes(anotherRowId),
                                   fam,
                                   qual,
                                   neverendingTxAboveLowWatermark.getStartTimestamp(),
                                   new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                HBaseUtils.hasShadowCell(Bytes.toBytes(anotherRowId),
                                         fam,
                                         qual,
                                         neverendingTxAboveLowWatermark.getStartTimestamp(),
                                         new TTableCellGetterAdapter(txTable)));

        assertEquals("Rows in table before flushing should be 2", 2, rowCount(TABLE_NAME, fam));
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);
        assertEquals("Rows in table after flushing should be 2", 2, rowCount(TABLE_NAME, fam));

        // Return a LWM that triggers compaction and stays between both ST of TXs, so assign 1st TX's start timestamp
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
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");

        // One row should have been discarded after compacting
        assertEquals("There should be only one row in table after compacting", 1, rowCount(TABLE_NAME, fam));
        // The row from the TX below the LWM should not be there (nor its Shadow Cell)
        assertFalse("Cell should not be there",
                HBaseUtils.hasCell(Bytes.toBytes(rowId),
                                   fam,
                                   qual,
                                   neverendingTxBelowLowWatermark.getStartTimestamp(),
                                   new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                HBaseUtils.hasShadowCell(Bytes.toBytes(rowId),
                                         fam,
                                         qual,
                                         neverendingTxBelowLowWatermark.getStartTimestamp(),
                                         new TTableCellGetterAdapter(txTable)));
        // The row from the TX above the LWM should be there without the Shadow Cell
        assertTrue("Cell should be there",
                HBaseUtils.hasCell(Bytes.toBytes(anotherRowId),
                                   fam,
                                   qual,
                                   neverendingTxAboveLowWatermark.getStartTimestamp(),
                                   new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                HBaseUtils.hasShadowCell(Bytes.toBytes(anotherRowId),
                                         fam,
                                         qual,
                                         neverendingTxAboveLowWatermark.getStartTimestamp(),
                                         new TTableCellGetterAdapter(txTable)));

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
                HBaseUtils.hasCell(Bytes.toBytes(rowId),
                                   fam,
                                   qual,
                                   neverendingTx.getStartTimestamp(),
                                   new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                HBaseUtils.hasShadowCell(Bytes.toBytes(rowId),
                                         fam,
                                         qual,
                                         neverendingTx.getStartTimestamp(),
                                         new TTableCellGetterAdapter(txTable)));

        assertEquals("There should be only one rows in table before flushing", 1, rowCount(TABLE_NAME, fam));
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);
        assertEquals("There should be only one rows in table after flushing", 1, rowCount(TABLE_NAME, fam));

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
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");

        // All rows should be there after the failed compaction
        assertEquals("There should be only one row in table after compacting", 1, rowCount(TABLE_NAME, fam));
        assertTrue("Cell should be there",
                HBaseUtils.hasCell(Bytes.toBytes(rowId),
                                   fam,
                                   qual,
                                   neverendingTx.getStartTimestamp(),
                                   new TTableCellGetterAdapter(txTable)));
        assertFalse("Shadow cell should not be there",
                HBaseUtils.hasShadowCell(Bytes.toBytes(rowId),
                                         fam,
                                         qual,
                                         neverendingTx.getStartTimestamp(),
                                         new TTableCellGetterAdapter(txTable)));
    }

    @Test
    public void testOriginalTableParametersAreAvoidedAlsoWhenCompacting() throws Throwable {

        long rowId = randomGenerator.nextLong();
        for (int versionCount = 0; versionCount <= (2 * MAX_VERSIONS); versionCount++) {
            Transaction tx = tm.begin();
            Put put = new Put(Bytes.toBytes(rowId));
            put.add(fam, qual, Bytes.toBytes("testWrite-" + versionCount));
            txTable.put(tx, put);
            tm.commit(tx);
        }

        Transaction tx = tm.begin();
        Get get = new Get(Bytes.toBytes(rowId));
        get.setMaxVersions(2 * MAX_VERSIONS);
        assertEquals("Max versions should be set to " + (2 * MAX_VERSIONS), (2 * MAX_VERSIONS), get.getMaxVersions());
        get.addColumn(fam, qual);
        Result result = txTable.get(tx, get);
        tm.commit(tx);
        List<Cell> column = result.getColumnCells(fam, qual);
        assertEquals("There should be only one version in the result", 1, column.size());

        assertEquals("There should be only one row in table before flushing", 1, rowCount(TABLE_NAME, fam));
        LOG.info("Flushing table {}", TEST_TABLE);
        admin.flush(TEST_TABLE);
        assertEquals("There should be only one row in table after flushing", 1, rowCount(TABLE_NAME, fam));

        // Return a LWM that triggers compaction
        compactEverything();

        // One row should have been discarded after compacting
        assertEquals("There should be only one row in table after compacting", 1, rowCount(TABLE_NAME, fam));

        tx = tm.begin();
        get = new Get(Bytes.toBytes(rowId));
        get.setMaxVersions(2 * MAX_VERSIONS);
        assertEquals("Max versions should be set to " + (2 * MAX_VERSIONS), (2 * MAX_VERSIONS), get.getMaxVersions());
        get.addColumn(fam, qual);
        result = txTable.get(tx, get);
        tm.commit(tx);
        column = result.getColumnCells(fam, qual);
        assertEquals("There should be only one version in the result", 1, column.size());
        assertEquals("Values don't match",
                     "testWrite-" + (2 * MAX_VERSIONS),
                     Bytes.toString(CellUtil.cloneValue(column.get(0))));
    }

    @Test(timeout=60000)
    public void testNonOmidCFIsUntouched() throws Throwable {
        admin.disableTable(TEST_TABLE);
        byte[] nonOmidCF = Bytes.toBytes("nonOmidCF");
        byte[] nonOmidQual = Bytes.toBytes("nonOmidCol");
        HColumnDescriptor nonomidfam = new HColumnDescriptor(nonOmidCF);
        nonomidfam.setMaxVersions(MAX_VERSIONS);
        admin.addColumn(TEST_TABLE, nonomidfam);
        admin.enableTable(TEST_TABLE);

        byte[] rowId = Bytes.toBytes("testRow");
        Transaction tx = tm.begin();
        Put put = new Put(rowId);
        put.add(fam, qual, Bytes.toBytes("testValue"));
        txTable.put(tx, put);

        Put nonTxPut = new Put(rowId);
        nonTxPut.add(nonOmidCF, nonOmidQual, Bytes.toBytes("nonTxVal"));
        txTable.getHTable().put(nonTxPut);
        txTable.flushCommits(); // to make sure it left the client

        Get g = new Get(rowId);
        Result result = txTable.getHTable().get(g);
        assertEquals("Should be there, precompact",
                1, result.getColumnCells(nonOmidCF, nonOmidQual).size());
        assertEquals("Should be there, precompact",
                1, result.getColumnCells(fam, qual).size());

        compactEverything();

        result = txTable.getHTable().get(g);
        assertEquals("Should be there, postcompact",
                1, result.getColumnCells(nonOmidCF, nonOmidQual).size());
        assertEquals("Should not be there, postcompact",
                0, result.getColumnCells(fam, qual).size());
    }

    private void compactEverything() throws Exception {
        admin.flush(TEST_TABLE);

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
        Thread.sleep(3000);
        LOG.info("Waking up after 3 secs");
    }

    private static long rowCount(TableName table, byte[] family) throws Throwable {
        Scan scan = new Scan();
        scan.addFamily(family);
        return aggregationClient.rowCount(table, new LongColumnInterpreter(), scan);
    }

}
