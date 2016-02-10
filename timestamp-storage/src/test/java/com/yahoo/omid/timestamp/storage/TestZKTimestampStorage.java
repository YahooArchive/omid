package com.yahoo.omid.timestamp.storage;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.BindException;

import static com.yahoo.omid.timestamp.storage.ZKTimestampPaths.TIMESTAMP_ZNODE;
import static com.yahoo.omid.timestamp.storage.ZKTimestampStorage.INITIAL_MAX_TS_VALUE;
import static org.mockito.Mockito.doThrow;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestZKTimestampStorage {

    private static final Logger LOG = LoggerFactory.getLogger(TestZKTimestampStorage.class);

    private static final int BYTES_IN_LONG = 8;

    private static final int ZK_PORT = 16666;
    private static final String ZK_CLUSTER = "localhost:" + ZK_PORT;

    private static final long NEGATIVE_TS = -1;

    private static final int ITERATION_COUNT = 10;

    private TestingServer zkServer;

    private CuratorFramework zkClient;

    private ZKTimestampStorage storage;

    private CuratorFramework storageInternalZKClient;

    @BeforeMethod
    public void initStuff() throws Exception {
        LOG.info("Creating ZK server instance listening in port {}...", ZK_PORT);
        while (zkServer == null) {
            try {
                zkServer = new TestingServer(ZK_PORT);
            } catch (BindException e) {
                System.err.println("Getting bind exception - retrying to allocate server");
                zkServer = null;
            }
        }
        LOG.info("ZK Server Started @ {}", zkServer.getConnectString());

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        LOG.info("Creating Zookeeper Client connected to {}", ZK_CLUSTER);
        zkClient = CuratorFrameworkFactory.builder()
                .namespace("omid")
                .connectString(ZK_CLUSTER)
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(10) // Low timeout for tests
                .build();
        zkClient.start();
        zkClient.blockUntilConnected();

        LOG.info("Creating Internal Zookeeper Client connected to {}", ZK_CLUSTER);
        storageInternalZKClient = Mockito.spy(CuratorFrameworkFactory.builder()
                .namespace("omid")
                .connectString(ZK_CLUSTER)
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(10) // Low timeout for tests
                .build());
        storageInternalZKClient.start();
        storageInternalZKClient.blockUntilConnected();

        storage = new ZKTimestampStorage(storageInternalZKClient);
    }

    @AfterMethod
    public void closeStuff() throws Exception {

        CloseableUtils.closeQuietly(zkClient);
        LOG.info("ZK Client state {}", zkClient.getState());
        zkClient = null;

        CloseableUtils.closeQuietly(storageInternalZKClient);
        LOG.info("ZK Internal Client state {}", storageInternalZKClient.getState());
        storageInternalZKClient = null;

        CloseableUtils.closeQuietly(zkServer);
        LOG.info("ZK Server Stopped");
        zkServer = null;

    }

    @Test
    public void testBasicFunctionality() throws Exception {

        // Check ZNode for timestamp exists (storage instantiation should create it)
        Stat zNodeStats = zkClient.checkExists().forPath(TIMESTAMP_ZNODE);
        assertEquals(zNodeStats.getVersion(), 0);

        // Initial checks
        assertEquals(storage.getMaxTimestamp(), INITIAL_MAX_TS_VALUE);
        byte[] data = zkClient.getData().forPath(TIMESTAMP_ZNODE);
        assertEquals(data.length, BYTES_IN_LONG);

        // Check new timestamp does not allow negative values...
        try {
            storage.updateMaxTimestamp(INITIAL_MAX_TS_VALUE, NEGATIVE_TS);
            fail();
        } catch (IllegalArgumentException e) {
            // Expected exception
        }

        // ...nor is less than previous timestamp
        try {
            storage.updateMaxTimestamp(1, 0);
            fail();
        } catch (IllegalArgumentException e) {
            // Expected exception
        }

        // Check that the original version is still there
        zNodeStats = zkClient.checkExists().forPath(TIMESTAMP_ZNODE);
        assertEquals(zNodeStats.getVersion(), 0);

        // Iterate updating the timestamp and check the final value
        long previousMaxTimestamp = INITIAL_MAX_TS_VALUE;
        for (int i = 0; i < ITERATION_COUNT; i++) {
            long newMaxTimestamp = previousMaxTimestamp + 1_000_000;
            storage.updateMaxTimestamp(previousMaxTimestamp, newMaxTimestamp);
            previousMaxTimestamp = newMaxTimestamp;
        }
        assertEquals(storage.getMaxTimestamp(), 1_000_000 * ITERATION_COUNT);
        // Check the znode version has changed accordingly
        zNodeStats = zkClient.checkExists().forPath(TIMESTAMP_ZNODE);
        assertEquals(zNodeStats.getVersion(), ITERATION_COUNT);

        // Check exceptions
        doThrow(new RuntimeException()).when(storageInternalZKClient).getData();
        try {
            storage.getMaxTimestamp();
            fail();
        } catch (IOException e) {
            // Expected exception
        }

        doThrow(new RuntimeException()).when(storageInternalZKClient).setData();
        try {
            storage.updateMaxTimestamp(INITIAL_MAX_TS_VALUE, INITIAL_MAX_TS_VALUE + 1_000_000);
            fail();
        } catch (IOException e) {
            // Expected exception
        }

        // Reset the mock and double-check last result
        Mockito.reset(storageInternalZKClient);
        assertEquals(storage.getMaxTimestamp(), 1_000_000 * ITERATION_COUNT);

        // Finally check the znode version is still the same
        zNodeStats = zkClient.checkExists().forPath(TIMESTAMP_ZNODE);
        assertEquals(zNodeStats.getVersion(), ITERATION_COUNT);
    }

    @Test
    public void testZkClientWhenZKIsDownAndRestarts() throws Exception {

        // Iterate updating the timestamp and check the final value
        long previousMaxTimestamp = INITIAL_MAX_TS_VALUE;
        for (int i = 0; i < ITERATION_COUNT; i++) {
            long newMaxTimestamp = previousMaxTimestamp + 1_000_000;
            storage.updateMaxTimestamp(previousMaxTimestamp, newMaxTimestamp);
            previousMaxTimestamp = newMaxTimestamp;
        }
        assertEquals(storage.getMaxTimestamp(), 1_000_000 * ITERATION_COUNT);

        // Stop ZK Server, expect the IO exception, reconnect and get the right value
        LOG.info("Stopping ZK Server");
        zkServer.stop();
        LOG.info("ZK Server Stopped");

        try {
            storage.getMaxTimestamp();
            fail();
        } catch (IOException ioe) {
            // Expected exception
        }

        LOG.info("Restarting ZK again");
        zkServer.restart();
        assertEquals(storage.getMaxTimestamp(), 1_000_000 * ITERATION_COUNT);

    }

    @Test
    public void testZkClientLosingSession() throws Exception {

        // Cut the session in the server through the client
        long sessionId = zkClient.getZookeeperClient().getZooKeeper().getSessionId();
        byte[] sessionPasswd = zkClient.getZookeeperClient().getZooKeeper().getSessionPasswd();
        ZooKeeper zk = new ZooKeeper(ZK_CLUSTER, 1000, null, sessionId, sessionPasswd);
        zk.close();
        LOG.info("ZKClient session closed");

        // Iterate updating the timestamp and check the final value
        long previousMaxTimestamp = INITIAL_MAX_TS_VALUE;
        for (int i = 0; i < ITERATION_COUNT; i++) {
            long newMaxTimestamp = previousMaxTimestamp + 1_000_000;
            storage.updateMaxTimestamp(previousMaxTimestamp, newMaxTimestamp);
            LOG.info("Updating timestamp. Previous/New {}/{}", previousMaxTimestamp, newMaxTimestamp);
            previousMaxTimestamp = newMaxTimestamp;
        }
        assertEquals(storage.getMaxTimestamp(), 1_000_000 * ITERATION_COUNT);

    }
}
