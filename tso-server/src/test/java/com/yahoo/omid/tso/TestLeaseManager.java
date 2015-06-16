package com.yahoo.omid.tso;

import static com.yahoo.omid.ZKConstants.OMID_NAMESPACE;
import static com.yahoo.omid.tsoclient.TSOClient.DEFAULT_ZK_CLUSTER;
import static org.testng.Assert.assertEquals;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;

public class TestLeaseManager {

    private static final long DUMMY_EPOCH = 1L;

    private static final String LEASE_MGR_ID_1 = "LM1";
    private static final String LEASE_MGR_ID_2 = "LM2";
    private static final String INSTANCE_ID_1 = "LM1" + "#" + DUMMY_EPOCH;
    private static final String INSTANCE_ID_2 = "LM2" + "#" + DUMMY_EPOCH;

    private static final Logger LOG = LoggerFactory.getLogger(TestLeaseManager.class);

    private static final long TEST_LEASE_PERIOD_IN_MS = 2 * 1000;

    private CuratorFramework zkClient;
    private TestingServer zkServer;

    private PausableLeaseManager leaseManager1;
    private PausableLeaseManager leaseManager2;

    @BeforeClass
    public void beforeClass() throws Exception {

        LOG.info("Starting ZK Server");
        zkServer = provideZookeeperServer();
        LOG.info("ZK Server Started @ {}", zkServer.getConnectString());

        zkClient = provideInitializedZookeeperClient();

    }

    @AfterClass
    public void afterClass() throws Exception {

        zkClient.close();

        CloseableUtils.closeQuietly(zkServer);
        zkServer = null;
        LOG.info("ZK Server Stopped");

    }

    @Test(timeOut = 60000)
    public void testLeaseHolderDoesNotChangeWhenPausedForALongTimeAndTheresNoOtherInstance()
            throws Exception
    {

        final String TEST_TSO_LEASE_PATH = "/test1_tsolease";
        final String TEST_CURRENT_TSO_PATH = "/test1_currenttso";

        // Launch the instance under test...
        leaseManager1 = new PausableLeaseManager(LEASE_MGR_ID_1,
                                                 DUMMY_EPOCH,
                                                 TEST_LEASE_PERIOD_IN_MS,
                                                 TEST_TSO_LEASE_PATH,
                                                 TEST_CURRENT_TSO_PATH,
                                                 zkClient);
        leaseManager1.startAndWait();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... check is the lease holder
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1);

        // Then, pause instance when trying to renew lease...
        leaseManager1.pausedInTryToRenewLeasePeriod();

        // ...let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ...check that nothing changed...
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1);

        // Finally, resume the instance...
        leaseManager1.resume();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... and check again that nothing changed
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1);

    }

    @Test(timeOut = 60000)
    public void testLeaseHolderDoesNotChangeWhenANewLeaseManagerIsUp() throws Exception {

        final String TEST_TSO_LEASE_PATH = "/test2_tsolease";
        final String TEST_CURRENT_TSO_PATH = "/test2_currenttso";

        // Launch the master instance...
        leaseManager1 = new PausableLeaseManager(LEASE_MGR_ID_1,
                                                 DUMMY_EPOCH,
                                                 TEST_LEASE_PERIOD_IN_MS,
                                                 TEST_TSO_LEASE_PATH,
                                                 TEST_CURRENT_TSO_PATH,
                                                 zkClient);

        leaseManager1.startAndWait();

        // ...let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ...so it should be the current holder of the lease
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1);

        // Then launch another instance...
        leaseManager2 = new PausableLeaseManager(LEASE_MGR_ID_2,
                                                 DUMMY_EPOCH,
                                                 TEST_LEASE_PERIOD_IN_MS,
                                                 TEST_TSO_LEASE_PATH,
                                                 TEST_CURRENT_TSO_PATH,
                                                 zkClient);
        leaseManager2.startAndWait();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... and after the period, the first instance should be still the holder
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1);
    }

    @Test(timeOut = 60000)
    public void testLeaseHolderChangesWhenActiveLeaseManagerIsPaused() throws Exception {

        final String TEST_TSO_LEASE_PATH = "/test3_tsolease";
        final String TEST_CURRENT_TSO_PATH = "/test3_currenttso";

        // Launch the master instance...
        leaseManager1 = new PausableLeaseManager(LEASE_MGR_ID_1,
                                                 DUMMY_EPOCH,
                                                 TEST_LEASE_PERIOD_IN_MS,
                                                 TEST_TSO_LEASE_PATH,
                                                 TEST_CURRENT_TSO_PATH,
                                                 zkClient);

        leaseManager1.startAndWait();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... so it should be the current holder of the lease
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1);

        // Then launch another instance...
        leaseManager2 = new PausableLeaseManager(LEASE_MGR_ID_2,
                                                 DUMMY_EPOCH,
                                                 TEST_LEASE_PERIOD_IN_MS,
                                                 TEST_TSO_LEASE_PATH,
                                                 TEST_CURRENT_TSO_PATH,
                                                 zkClient);
        leaseManager2.startAndWait();

        // ... and pause active lease manager...
        leaseManager1.pausedInStillInLeasePeriod();

        // ... and let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... and check that lease owner should have changed to the second instance
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_2);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_2);

        // Now, lets resume the first instance...
        leaseManager1.resume();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // and check the lease owner is still the second instance (preserves the lease)
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_2);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_2);

        // Finally, pause active lease manager when trying to renew lease...
        leaseManager2.pausedInTryToRenewLeasePeriod();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... and check lease owner is has changed again to the first instance
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1);

        // Resume the second instance...
        leaseManager2.resume();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... but the lease owner should still be the first instance
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1);

    }

    // **************************** Checkers **********************************

    private void checkLeaseHolder(String tsoLeasePath, String expectedLeaseHolder) throws Exception {
        byte[] leaseHolderInBytes = zkClient.getData().forPath(tsoLeasePath);
        String leaseHolder = new String(leaseHolderInBytes, Charsets.UTF_8);

        assertEquals(leaseHolder, expectedLeaseHolder);
    }

    private void checkInstanceId(String currentTSOPath, String expectedInstanceId) throws Exception {
        byte[] expectedInstanceIdInBytes = zkClient.getData().forPath(currentTSOPath);
        String instanceId = new String(expectedInstanceIdInBytes, Charsets.UTF_8);

        assertEquals(instanceId, expectedInstanceId);
    }

    // **************************** Helpers ***********************************

    private static String ZK_CLUSTER = DEFAULT_ZK_CLUSTER;

    private static CuratorFramework provideInitializedZookeeperClient() throws Exception {

        LOG.info("Creating Zookeeper Client connecting to {}", ZK_CLUSTER);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework zkClient = CuratorFrameworkFactory
                .builder()
                .namespace(OMID_NAMESPACE)
                .connectString(ZK_CLUSTER)
                .retryPolicy(retryPolicy).build();

        LOG.info("Connecting to ZK cluster {}", zkClient.getState());
        zkClient.start();
        zkClient.blockUntilConnected();
        LOG.info("Connection to ZK cluster {}", zkClient.getState());

        return zkClient;
    }

    private static TestingServer provideZookeeperServer() throws Exception {
        LOG.info("Creating ZK server instance...");
        return new TestingServer(Integer.parseInt(ZK_CLUSTER.split(":")[1]));
    }

}
