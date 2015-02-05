package com.yahoo.omid.committable.hbase;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.apache.bookkeeper.client.LedgerFuture;
import org.apache.bookkeeper.client.LedgerSet.MetadataStorage;
import org.apache.bookkeeper.client.LedgerSet.MetadataStorage.BadVersionException;
import org.apache.bookkeeper.client.LedgerSet.MetadataStorage.ClosedException;
import org.apache.bookkeeper.client.LedgerSet.MetadataStorage.NoKeyException;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Version.Occurred;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

@Guice(modules = TestZkBasedMetadataStorage.TestModule.class)
public class TestZkBasedMetadataStorage {

    public static final byte[] TEST_DATA = "deadbeef".getBytes();

    public static final int INITAL_VERSION_VALUE = 0;
    public static final int NONEXISTING_VERSION_VALUE = 1000;

    public static final String LEDGERSET = "test-ls";

    public static class TestModule extends AbstractModule {

        @Override
        protected void configure() {
        }

        @Provides
        CuratorFramework provideZookeeperClient()
        {
            String zkCluster = (new HBaseCommitTableTester.Config()).zkCluster;
            LOG.info("Creating Zookeeper Client connected to {}", zkCluster);
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            return CuratorFrameworkFactory.builder()
                    .namespace("omid")
                    .connectString(zkCluster)
                    .retryPolicy(retryPolicy)
                    .build();
        }

        @Provides
        TestingServer provideZookeeperServer() throws Exception {
            LOG.info("Creating ZK server instance...");
            return new TestingServer(
                    Integer.parseInt((new HBaseCommitTableTester.Config()).zkCluster.split(":")[1]));
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(TestZkBasedMetadataStorage.class);

    @Inject
    private TestingServer zkServer;

    @Inject
    private CuratorFramework zkClient;

    @BeforeClass
    public void beforeClass() throws Exception {
        LOG.info("ZK Server Started @ {}", zkServer.getConnectString());
    }

    @AfterClass
    public void afterClass() throws Exception {
        zkServer.stop();
        CloseableUtils.closeQuietly(zkServer);
        LOG.info("ZK Server Stopped");
    }

    @Test(timeOut = 10000)
    public void testBasicOperations() throws Exception {
        MetadataStorage zkMetadataStorage = new ZKBasedMetadataStorage(zkClient);

        // Test unsuccessful read of uninitialized log
        LedgerFuture<Versioned<byte[]>> dataFuture = zkMetadataStorage.read(LEDGERSET);
        try {
            dataFuture.get();
        } catch (ExecutionException e) {
            // Expected
            assertTrue(e.getCause() instanceof NoKeyException);
        }

        // Test writing initial version
        LedgerFuture<Version> versionFuture = zkMetadataStorage.write(LEDGERSET, TEST_DATA, Version.NEW);
        Version currentVersion = versionFuture.get();
        assertEquals(currentVersion.compare(Version.NEW), Occurred.AFTER);
        // Check data and version reading directly from ZK
        Stat stat = new Stat();
        byte[] data = zkClient.getData().storingStatIn(stat).forPath(LEDGERSET);
        assertEquals(stat.getVersion(), INITAL_VERSION_VALUE);
        assertTrue(Arrays.equals(data, TEST_DATA));
        LOG.info("Version {} Data {}", stat.getVersion(), Arrays.toString(data));

        // Test successful repeated reads/writes
        for (int i = INITAL_VERSION_VALUE; i < (INITAL_VERSION_VALUE + 100); i++) {
            // Read previous versioned data
            dataFuture = zkMetadataStorage.read(LEDGERSET);
            Versioned<byte[]> versionedData = dataFuture.get();
            assertTrue(versionedData.getVersion().equals(currentVersion));
            assertTrue(Arrays.equals(versionedData.getValue(), TEST_DATA));
            Version readVersion = versionedData.getVersion();
            int readVersionAsInt = ((ZKBasedMetadataStorage.ZookeeperVersion) readVersion).getVersion();
            LOG.info("Current version read {}", readVersionAsInt);
            assertEquals(readVersionAsInt, i);
            // Write next and compare with read version
            versionFuture = zkMetadataStorage.write(LEDGERSET, TEST_DATA, currentVersion);
            currentVersion = versionFuture.get();
            assertEquals(currentVersion.compare(readVersion), Occurred.AFTER);
        }

        // Check data also reading directly from ZK
        data = zkClient.getData().storingStatIn(stat).forPath(LEDGERSET);
        assertEquals(stat.getVersion(), INITAL_VERSION_VALUE + 100);
        assertTrue(Arrays.equals(data, TEST_DATA));
        LOG.info("Version {} Data {}", stat.getVersion(), Arrays.toString(data));

        // Test delete nonexisting version
        Version nonExistingVersion = new ZKBasedMetadataStorage.ZookeeperVersion(NONEXISTING_VERSION_VALUE);
        LedgerFuture<Void> deleteFuture = zkMetadataStorage.delete(LEDGERSET, nonExistingVersion);
        try {
            deleteFuture.get();
        } catch (ExecutionException e) {
            // Expected
            assertTrue(e.getCause() instanceof BadVersionException);
        }
        // Test delete current version
        deleteFuture = zkMetadataStorage.delete(LEDGERSET, currentVersion);
        deleteFuture.get();
        // Read current versioned data
        dataFuture = zkMetadataStorage.read(LEDGERSET);
        try {
            dataFuture.get();
        } catch (ExecutionException e) {
            // Expected
            assertTrue(e.getCause() instanceof NoKeyException);
        }

        // Finally test metadata storage close
        dataFuture = zkMetadataStorage.read(LEDGERSET);
        ZKBasedMetadataStorage zkMTS = (ZKBasedMetadataStorage) zkMetadataStorage;
        assertEquals(zkMTS.outstandingCallbackFutures.size(), 1);
        zkMetadataStorage.close();
        assertEquals(zkMTS.outstandingCallbackFutures.size(), 0);
        try {
            dataFuture.get();
        } catch (ExecutionException e) {
            // Expected
            assertTrue(e.getCause() instanceof ClosedException);
        }

    }

}
