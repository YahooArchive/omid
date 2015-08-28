package com.yahoo.omid.transaction;

import static com.yahoo.omid.ZKConstants.CURRENT_TSO_PATH;
import static com.yahoo.omid.ZKConstants.OMID_NAMESPACE;
import static com.yahoo.omid.ZKConstants.TSO_LEASE_PATH;
import static com.yahoo.omid.committable.hbase.HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.committable.hbase.HBaseCommitTable.COMMIT_TABLE_FAMILY;
import static com.yahoo.omid.committable.hbase.HBaseCommitTable.LOW_WATERMARK_FAMILY;
import static com.yahoo.omid.tso.RequestProcessorImpl.TSO_MAX_ITEMS_KEY;
import static com.yahoo.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;
import static com.yahoo.omid.tso.hbase.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.tso.hbase.HBaseTimestampStorage.TSO_FAMILY;
import static com.yahoo.omid.tsoclient.TSOClient.TSO_ZK_CLUSTER_CONFKEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import com.yahoo.omid.timestamp.storage.TimestampStorage;
import com.yahoo.omid.tso.DisruptorModule;
import com.yahoo.omid.tso.LeaseManagement;
import com.yahoo.omid.tso.LeaseManagement.LeaseManagementException;
import com.yahoo.omid.tso.MockPanicker;
import com.yahoo.omid.tso.Panicker;
import com.yahoo.omid.tso.PausableLeaseManager;
import com.yahoo.omid.tso.PausableTimestampOracle;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import com.yahoo.omid.tso.TSOStateManager;
import com.yahoo.omid.tso.TSOStateManagerImpl;
import com.yahoo.omid.tso.TimestampOracle;
import com.yahoo.omid.tso.ZKModule;
import com.yahoo.omid.tso.hbase.HBaseTimestampStorage;
import com.yahoo.omid.tsoclient.TSOClient;

public class TestEndToEndScenariosWithHA {

    private static final int TEST_LEASE_PERIOD_MS = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(TestEndToEndScenariosWithHA.class);

    static final byte[] table = Bytes.toBytes("test-table");
    static final byte[] family = Bytes.toBytes("test-family");
    static final byte[] qualifier = Bytes.toBytes("test-qualifier");
    static final byte[] qualifier1 = Bytes.toBytes("test-q1");
    static final byte[] qualifier2 = Bytes.toBytes("test-q2l");
    static final byte[] row1 = Bytes.toBytes("row1");
    static final byte[] row2 = Bytes.toBytes("row2");
    static final byte[] initialData = Bytes.toBytes("testWrite-0");
    static final byte[] data1_q1 = Bytes.toBytes("testWrite-1-q1");
    static final byte[] data1_q2 = Bytes.toBytes("testWrite-1-q2");
    static final byte[] data2_q1 = Bytes.toBytes("testWrite-2-q1");
    static final byte[] data2_q2 = Bytes.toBytes("testWrite-2-q2");
    static final byte[] data1 = Bytes.toBytes("testWrite-1");
    static final byte[] data2 = Bytes.toBytes("testWrite-2");

    private CountDownLatch barrierTillTSOAddressPublication;

    protected static HBaseTestingUtility hBaseUtils;
    private static MiniHBaseCluster hBaseCluster;
    private Configuration hbaseClusterConfig;

    private CuratorFramework zkClient;

    private TSOServer tso1;
    private TSOServer tso2;

    private PausableLeaseManager leaseManager1;
    private PausableLeaseManager leaseManager2;

    private CommitTable.Client commitTableClient;
    private CommitTable.Writer commitTableWriter;

    private TSOClient tsoClientForTM;
    private TransactionManager tm;

    private String zkConnection;

    @BeforeClass
    public void beforeClass() throws Exception {

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 100 * 1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);
        hBaseUtils = new HBaseTestingUtility(hbaseConf);
        hBaseCluster = hBaseUtils.startMiniCluster(3);
        hBaseCluster.waitForActiveAndReadyMaster();
        hbaseClusterConfig = hBaseCluster.getConfiguration();

        LOG.info("+++++++++++++++++++ HBase started ++++++++++++++++++++++++");

        // Get the zkConnection string from minicluster
        zkConnection = "localhost:" + hBaseUtils.getZkCluster().getClientPort();

        zkClient = provideInitializedZookeeperClient(zkConnection);

    }

    @AfterClass
    public void afterClass() throws Exception {

        zkClient.close();

        hBaseUtils.shutdownMiniCluster();

    }

    @BeforeMethod
    public void setup() throws Exception {
        LOG.info("==========================================================");
        LOG.info("==========================================================");
        LOG.info("===================== Init Experiment ====================");
        LOG.info("==========================================================");
        LOG.info("==========================================================");

        Thread.currentThread().setName("Test Thread");

        try {
            zkClient.delete().forPath("/omid/omid/timestamp");
            LOG.info("Timestamp ZKPath {}", "/omid/omid/timestamp");
        } catch (Exception e) {
            LOG.warn("Timestamp ZKPath {} didn't exist", "/omid/omid/timestamp");
        }

        // Create commit table
        hBaseUtils.createTable(Bytes.toBytes(COMMIT_TABLE_DEFAULT_NAME),
                new byte[][] { COMMIT_TABLE_FAMILY, LOW_WATERMARK_FAMILY },
                Integer.MAX_VALUE);
        hBaseUtils.createTable(Bytes.toBytes(TIMESTAMP_TABLE_DEFAULT_NAME),
                new byte[][] { TSO_FAMILY }, Integer.MAX_VALUE);
        // Create tables for test
        hBaseUtils.createTable(table, new byte[][] { family }, Integer.MAX_VALUE);

        // Synchronize TSO start
        barrierTillTSOAddressPublication = new CountDownLatch(1);
        final NodeCache currentTSOZNode = new NodeCache(zkClient, CURRENT_TSO_PATH);
        currentTSOZNode.getListenable().addListener(new NodeCacheListener() {

            @Override
            public void nodeChanged() throws Exception {
                byte[] currentTSOAndEpochAsBytes = currentTSOZNode.getCurrentData().getData();
                String currentTSOAndEpoch = new String(currentTSOAndEpochAsBytes, Charsets.UTF_8);
                if (currentTSOAndEpoch.endsWith("#0")) { // Wait till a TSO instance publishes the epoch
                    barrierTillTSOAddressPublication.countDown();
                }
            }

        });
        currentTSOZNode.start(true);

        // Configure TSO 1
        TSOServerCommandLineConfig config1 = TSOServerCommandLineConfig.configFactory(1234, 1000);
        config1.shouldHostAndPortBePublishedInZK = true;
        config1.setZKCluster(zkConnection);
        config1.setLeasePeriodInMs(TEST_LEASE_PERIOD_MS);
        Injector injector1 = Guice.createInjector(new TestTSOModule(hbaseClusterConfig, config1));
        LOG.info("===================== Starting TSO 1 =====================");
        tso1 = injector1.getInstance(TSOServer.class);
        leaseManager1 = (PausableLeaseManager) injector1.getInstance(LeaseManagement.class);
        tso1.startAndWait();
        TestUtils.waitForSocketListening("localhost", 1234, 100);
        LOG.info("================ Finished loading TSO 1 ==================");

        CommitTable commitTable = injector1.getInstance(CommitTable.class);
        commitTableWriter = commitTable.getWriter().get();
        commitTableClient = commitTable.getClient().get();

        // Configure TSO 2
        TSOServerCommandLineConfig config2 = TSOServerCommandLineConfig.configFactory(4321, 1000);
        config2.shouldHostAndPortBePublishedInZK = true;
        config2.setZKCluster(zkConnection);
        config2.setLeasePeriodInMs(TEST_LEASE_PERIOD_MS);
        Injector injector2 = Guice.createInjector(new TestTSOModule(hbaseClusterConfig, config2));
        LOG.info("===================== Starting TSO 2 =====================");
        tso2 = injector2.getInstance(TSOServer.class);
        leaseManager2 = (PausableLeaseManager) injector2.getInstance(LeaseManagement.class);
        tso2.startAndWait();
        TestUtils.waitForSocketListening("localhost", 4321, 100);
        LOG.info("================ Finished loading TSO 2 ==================");

        // Wait till the master TSO is up
        barrierTillTSOAddressPublication.await();
        currentTSOZNode.close();

        // Configure HBase TM
        LOG.info("===================== Starting TM =====================");
        BaseConfiguration clientConf = new BaseConfiguration();
        clientConf.setProperty(TSO_ZK_CLUSTER_CONFKEY, zkConnection);
        tsoClientForTM = TSOClient.newBuilder().withConfiguration(clientConf).build();
        LOG.info("TSOClient instance in test {}", tsoClientForTM);
        Configuration hbaseConf = hBaseCluster.getConfiguration();
        hbaseConf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 3);
        tm = HBaseTransactionManager.newBuilder()
                                    .withTSOClient(tsoClientForTM)
                                    .withConfiguration(hbaseConf)
                                    .build();
        LOG.info("===================== TM Started =========================");

        LOG.info("++++++++++++++++++ Starting Experiment... ++++++++++++++++");
    }


    @AfterMethod
    public void cleanup() throws Exception {
        hBaseUtils.deleteTable(COMMIT_TABLE_DEFAULT_NAME);
        hBaseUtils.deleteTable(TIMESTAMP_TABLE_DEFAULT_NAME);
        hBaseUtils.deleteTable(table);
        tso1.stopAndWait();
        tso2.stopAndWait();
    }

    //
    // TSO 1 is MASTER & TSO 2 is BACKUP
    // Setup: TX 0 -> Add initial data to cells R1C1 (v0) & R2C2 (v0)
    // TX 1 starts (TSO1)
    // TX 1 modifies cells R1C1 & R2C2 (v1)
    // Interleaved Read TX -IR TX- starts (TSO1)
    // TSO 1 PAUSES -> TSO 2 becomes MASTER
    // IR TX reads R1C1 -> should get v0
    // TX 1 tries to commit -> should abort because was started in TSO 1
    // IR TX reads R2C2 -> should get v0
    // IR TX tries to commit -> should abort because was started in TSO 1
    // End of Test state: R1C1 & R2C2 (v0)
    @Test
    public void testScenario1() throws Exception {

        try (TTable txTable = new TTable(hBaseCluster.getConfiguration(), table)) {

            // Write initial values for the test
            HBaseTransaction tx0 = (HBaseTransaction) tm.begin();
            LOG.info("Starting Tx {} writing initial values for cells ({}) ", Bytes.toString(initialData));
            Put putInitialDataRow1 = new Put(row1);
            putInitialDataRow1.add(family, qualifier1, initialData);
            txTable.put(tx0, putInitialDataRow1);
            Put putInitialDataRow2 = new Put(row2);
            putInitialDataRow2.add(family, qualifier2, initialData);
            txTable.put(tx0, putInitialDataRow2);
            tm.commit(tx0);

            // Initial checks
            checkRowValues(txTable, initialData, initialData);

            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            LOG.info("Starting Tx {} writing values for cells ({}, {}) ", tx1, Bytes.toString(data1_q1),
                    Bytes.toString(data1_q2));
            Put putData1R1Q1 = new Put(row1);
            putData1R1Q1.add(family, qualifier1, data1_q1);
            txTable.put(tx1, putData1R1Q1);
            Put putData1R2Q2 = new Put(row2);
            putData1R2Q2.add(family, qualifier2, data1_q2);
            txTable.put(tx1, putData1R2Q2);

            Transaction interleavedReadTx = tm.begin();

            LOG.info("Starting Interleaving Read Tx {} for checking cell values", interleavedReadTx.getTransactionId());

            // Simulate a GC pause to change mastership (should throw a ServiceUnavailable exception)
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            LOG.info("++++++++++++++++++++ PAUSING TSO 1 +++++++++++++++++++");
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            leaseManager1.pausedInStillInLeasePeriod();

            // Read interleaved and check the values writen by tx 1
            Get getRow1 = new Get(row1).setMaxVersions(1);
            getRow1.addColumn(family, qualifier1);
            Result r = txTable.get(interleavedReadTx, getRow1);
            assertEquals(r.getValue(family, qualifier1), initialData,
                    "Unexpected value for SI read R1Q1" + interleavedReadTx + ": "
                            + Bytes.toString(r.getValue(family, qualifier1)));

            // Try to commit, but it should abort due to the change in mastership
            try {
                tm.commit(tx1);
                fail();
            } catch (RollbackException e) {
                // Expected
                LOG.info("Rollback cause for Tx {}: ", tx1, e.getCause());
                assertEquals(tx1.getStatus(), Transaction.Status.ROLLEDBACK);
                assertEquals(tx1.getEpoch(), 0);
            }

            // Read interleaved and check the values writen by tx 1
            Get getRow2 = new Get(row2).setMaxVersions(1);
            r = txTable.get(interleavedReadTx, getRow2);
            assertEquals(r.getValue(family, qualifier2), initialData,
                    "Unexpected value for SI read R2Q2" + interleavedReadTx + ": "
                            + Bytes.toString(r.getValue(family, qualifier2)));

            try {
                tm.commit(interleavedReadTx);
                fail();
            } catch (RollbackException e) {
                // Expected
                // NOTE: This is a read only transaction (It's writeset is empty)
                // and probably it should not be aborted despite the change in
                // TSO mastership. We should take this into consideration for future improvements
                LOG.info("Rollback cause for Tx {}: ", interleavedReadTx, e.getCause());
                assertEquals(interleavedReadTx.getEpoch(), 0);
                assertEquals(interleavedReadTx.getStatus(), Transaction.Status.ROLLEDBACK);
            }

            LOG.info("Sleep the Lease period till the client is informed about"
                    + "the new TSO connection parameters and how can connect");
            Thread.sleep(3000);

            checkRowValues(txTable, initialData, initialData);

        }

    }

    //
    // TSO 1 is MASTER & TSO 2 is BACKUP
    // Setup: TX 0 -> Add initial data to cells R1C1 (v0) & R2C2 (v0)
    // TX 1 starts (TSO1)
    // TX 1 modifies cells R1C1 & R2C2 (v1)
    // TSO 1 is KILLED -> TSO 2 becomes MASTER
    // TX 1 tries to commit -> should abort because was started in TSO 1
    // TX 2 starts (TSO1)
    // TX 2 reads R1C1 -> should get v0
    // TX 2 reads R2C2 -> should get v0
    // TX 2 modifies cells R1C1 & R2C2 (v2)
    // TX 2 commits
    // End of Test state: R1C1 & R2C2 (v2)
    @Test
    public void testScenario2() throws Exception {

        try (TTable txTable = new TTable(hBaseCluster.getConfiguration(), table)) {

            // Write initial values for the test
            HBaseTransaction tx0 = (HBaseTransaction) tm.begin();
            LOG.info("Starting Tx {} writing initial values for cells ({}) ", Bytes.toString(initialData));
            Put putInitialDataRow1 = new Put(row1);
            putInitialDataRow1.add(family, qualifier1, initialData);
            txTable.put(tx0, putInitialDataRow1);
            Put putInitialDataRow2 = new Put(row2);
            putInitialDataRow2.add(family, qualifier2, initialData);
            txTable.put(tx0, putInitialDataRow2);
            tm.commit(tx0);

            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            LOG.info("Starting Tx {} writing values for cells ({}, {}) ", tx1, Bytes.toString(data1_q1),
                    Bytes.toString(data1_q2));
            Put putData1R1Q1 = new Put(row1);
            putData1R1Q1.add(family, qualifier1, data1_q1);
            txTable.put(tx1, putData1R1Q1);
            Put putData1R2Q2 = new Put(row2);
            putData1R2Q2.add(family, qualifier2, data1_q2);
            txTable.put(tx1, putData1R2Q2);

            // Provoke change in mastership (should throw a Connection exception)
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            LOG.info("++++++++++++++++++++ KILLING TSO 1 +++++++++++++++++++");
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            LOG.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            tso1.stopAndWait();

            // Try to commit, but it should abort due to the change in mastership
            try {
                tm.commit(tx1);
                fail();
            } catch (RollbackException e) {
                // Expected
                LOG.info("Rollback cause for Tx {}: ", tx1, e.getCause());
                assertEquals(tx1.getStatus(), Transaction.Status.ROLLEDBACK);
                assertEquals(tx1.getEpoch(), 0);
            }

            LOG.info("Sleep some time till the client is informed about"
                    + "the new TSO connection parameters and how can connect");
            Thread.sleep(5000);

            HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
            LOG.info("Starting Tx {} writing values for cells ({}, {}) ", tx2, Bytes.toString(data1_q1),
                    Bytes.toString(data1_q2));
            Get getData1R1Q1 = new Get(row1).setMaxVersions(1);
            Result r = txTable.get(tx2, getData1R1Q1);
            assertEquals(r.getValue(family, qualifier1), initialData,
                    "Unexpected value for SI read R1Q1" + tx2 + ": "
                            + Bytes.toString(r.getValue(family, qualifier1)));
            Get getData1R2Q2 = new Get(row2).setMaxVersions(1);
            r = txTable.get(tx2, getData1R2Q2);
            assertEquals(r.getValue(family, qualifier2), initialData,
                    "Unexpected value for SI read R1Q1" + tx2 + ": "
                            + Bytes.toString(r.getValue(family, qualifier2)));

            Put putData2R1Q1 = new Put(row1);
            putData2R1Q1.add(family, qualifier1, data2_q1);
            txTable.put(tx2, putData2R1Q1);
            Put putData2R2Q2 = new Put(row2);
            putData2R2Q2.add(family, qualifier2, data2_q2);
            txTable.put(tx2, putData2R2Q2);
            // This one should commit in the new TSO
            tm.commit(tx2);

            assertEquals(tx2.getStatus(), Transaction.Status.COMMITTED);
            final long TIMESTAMP_ORACLE_TIMESTAMP_BATCH = 10_000_000;
            assertEquals(tx2.getEpoch(), TIMESTAMP_ORACLE_TIMESTAMP_BATCH);

            checkRowValues(txTable, data2_q1, data2_q2);
        }

    }

    private void checkRowValues(TTable txTable, byte[] expectedDataR1Q1, byte[] expectedDataR2Q2)
            throws TransactionException, IOException, RollbackException {
        Transaction readTx = tm.begin();
        LOG.info("Starting Read Tx {} for checking cell values", readTx.getTransactionId());
        Get getRow1 = new Get(row1).setMaxVersions(1);
        getRow1.addColumn(family, qualifier1);
        Result r = txTable.get(readTx, getRow1);
        assertEquals(r.getValue(family, qualifier1), expectedDataR1Q1,
                "Unexpected value for SI read R1Q1" + readTx + ": " + Bytes.toString(r.getValue(family, qualifier1)));
        Get getRow2 = new Get(row2).setMaxVersions(1);
        r = txTable.get(readTx, getRow2);
        assertEquals(r.getValue(family, qualifier2), expectedDataR2Q2,
                "Unexpected value for SI read R2Q2" + readTx + ": " + Bytes.toString(r.getValue(family, qualifier2)));
        tm.commit(readTx);
    }

    // **************************** Helpers ***********************************


    private static CuratorFramework provideInitializedZookeeperClient(String zkConnection) throws Exception {

        LOG.info("Creating Zookeeper Client connecting to {}", zkConnection);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework zkClient = CuratorFrameworkFactory
                .builder()
                .namespace(OMID_NAMESPACE)
                .connectString(zkConnection)
                .retryPolicy(retryPolicy).build();

        LOG.info("Connecting to ZK cluster {}", zkClient.getState());
        zkClient.start();
        zkClient.blockUntilConnected();
        LOG.info("Connection to ZK cluster {}", zkClient.getState());

        return zkClient;
    }

    private TransactionManager newTransactionManager(Configuration clientConf) throws Exception {
        return HBaseTransactionManager.newBuilder()
                .withConfiguration(clientConf).build();
    }

    private static class TestTSOModule extends AbstractModule {

        private final Configuration hBaseConfig;
        private final TSOServerCommandLineConfig config;

        public TestTSOModule(Configuration hBaseConfig, TSOServerCommandLineConfig config) {
            this.hBaseConfig = hBaseConfig;
            this.config = config;
        }

        @Override
        protected void configure() {

            bind(TSOStateManager.class).to(TSOStateManagerImpl.class).in(Singleton.class);

            bind(CommitTable.class).to(HBaseCommitTable.class).in(Singleton.class);
            bind(TimestampStorage.class).to(HBaseTimestampStorage.class).in(Singleton.class);
            bind(TimestampOracle.class).to(PausableTimestampOracle.class).in(Singleton.class);
            bind(Panicker.class).to(MockPanicker.class).in(Singleton.class);

            // Disruptor setup
            // Overwrite default value
            bindConstant().annotatedWith(Names.named(TSO_MAX_ITEMS_KEY)).to(config.getMaxItems());
            install(new DisruptorModule());

            // ZK Module
            install(new ZKModule(config));

        }

        // LeaseManagement setup
        @Provides
        @Singleton
        LeaseManagement provideLeaseManager(@Named(TSO_HOST_AND_PORT_KEY) String tsoHostAndPort,
                                                                       TSOStateManager stateManager,
                                                                       CuratorFramework zkClient)
        throws LeaseManagementException {

            LOG.info("Connection to ZK cluster [{}]", zkClient.getState());
            return new PausableLeaseManager(tsoHostAndPort,
                                        stateManager,
                                        config.getLeasePeriodInMs(),
                                        TSO_LEASE_PATH,
                                        CURRENT_TSO_PATH,
                                        zkClient);
        }

        @Provides
        Configuration provideHBaseConfig() {
            return hBaseConfig;
        }

        @Provides
        TSOServerCommandLineConfig provideTSOServerConfig() {
            return config;
        }

        @Provides @Singleton
        MetricsRegistry provideMetricsRegistry() {
            return new NullMetricsProvider();
        }

    }
}
