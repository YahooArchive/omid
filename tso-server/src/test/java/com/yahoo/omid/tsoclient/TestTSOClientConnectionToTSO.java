package com.yahoo.omid.tsoclient;

import static com.yahoo.omid.ZKConstants.CURRENT_TSO_PATH;
import static com.yahoo.omid.ZKConstants.OMID_NAMESPACE;
import static com.yahoo.omid.tsoclient.TSOClient.DEFAULT_ZK_CLUSTER;
import static com.yahoo.omid.tsoclient.TSOClient.TSO_HOST_CONFKEY;
import static com.yahoo.omid.tsoclient.TSOClient.TSO_PORT_CONFKEY;
import static com.yahoo.omid.tsoclient.TSOClient.TSO_ZK_CLUSTER_CONFKEY;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.tso.TSOMockModule;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import com.yahoo.omid.tsoclient.TSOClient.ConnectionException;
import com.yahoo.statemachine.StateMachine.FsmImpl;

public class TestTSOClientConnectionToTSO {

    private static final Logger LOG = LoggerFactory.getLogger(TestTSOClientConnectionToTSO.class);

    private static final String TSO_HOST = "localhost";
    private static final int TSO_PORT = 12345;

    private Injector injector = null;

    private static TestingServer zkServer;

    private static CuratorFramework zkClient;

    private TSOServer tsoServer;

    @BeforeMethod
    public void beforeMethod() throws Exception {

        LOG.info("Starting ZK Server");
        zkServer = TestUtils.provideZookeeperServer();
        LOG.info("ZK Server Started @ {}", zkServer.getConnectString());

        zkClient = provideInitializedZookeeperClient();

        Stat stat;
        try {
            zkClient.delete().forPath(CURRENT_TSO_PATH);
            stat = zkClient.checkExists().forPath(CURRENT_TSO_PATH);
            assertNull(stat, CURRENT_TSO_PATH + " should not exist");
        } catch (NoNodeException e) {
            LOG.info("{} ZNode did not existed", CURRENT_TSO_PATH);
        }

    }

    @AfterMethod
    public void afterMethod() {

        zkClient.close();

        CloseableUtils.closeQuietly(zkServer);
        zkServer = null;
        LOG.info("ZK Server Stopped");

    }

    @Test
    public void testUnsuccessfulConnectionToTSO() throws Exception {

        Configuration clientConf = new BaseConfiguration();

        // When no ZK node for TSOServer is found & no host:port config exists
        // we should get an exception when getting the client
        try {
            TSOClient.newBuilder().withConfiguration(clientConf).build();
        } catch (IllegalArgumentException e) {
            // Expected
        }

    }

    @Test
    public void testSuccessfulConnectionToTSOWithHostAndPort() throws Exception {

        Configuration clientConf = new BaseConfiguration();
        clientConf.setProperty(TSO_HOST_CONFKEY, "localhost");
        clientConf.setProperty(TSO_PORT_CONFKEY, TSO_PORT);

        // Launch a TSO WITHOUT publishing the address in ZK...
        injector = Guice.createInjector(new TSOMockModule(TSOServerCommandLineConfig.configFactory(TSO_PORT, 1000)));
        LOG.info("Starting TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAndWait();
        TestUtils.waitForSocketListening(TSO_HOST, TSO_PORT, 100);
        LOG.info("Finished loading TSO");

        // When no ZK node for TSOServer is found we should get a connection
        // to the TSO through the host:port configured...
        TSOClient tsoClient = TSOClient.newBuilder().withConfiguration(clientConf).build();

        // ... so we should get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), 1);

        // Close the tsoClient connection and stop the TSO Server
        tsoClient.close().get();
        tsoServer.stopAndWait();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, TSO_PORT, 1000);
        LOG.info("TSO Server Stopped");

    }

    @Test
    public void testSuccessfulConnectionToTSOThroughZK() throws Exception {

        Configuration clientConf = new BaseConfiguration();
        clientConf.setProperty(TSO_ZK_CLUSTER_CONFKEY, DEFAULT_ZK_CLUSTER);

        // Launch a TSO publishing the address in ZK...
        TSOServerCommandLineConfig config = TSOServerCommandLineConfig.configFactory(TSO_PORT, 1000);
        config.shouldHostAndPortBePublishedInZK = true;
        config.setLeasePeriodInMs(1000);
        injector = Guice.createInjector(new TSOMockModule(config));
        LOG.info("Starting TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAndWait();
        TestUtils.waitForSocketListening(TSO_HOST, TSO_PORT, 100);
        LOG.info("Finished loading TSO");

        Thread.sleep(1500); // Allow the TSO to register

        // When a ZK node for TSOServer is found we should get a connection
        TSOClient tsoClient = TSOClient.newBuilder().withConfiguration(clientConf).build();

        // ... so we should get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), 1);

        // Close the tsoClient connection and stop the TSO Server
        tsoClient.close().get();
        tsoServer.stopAndWait();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, TSO_PORT, 1000);
        LOG.info("TSO Server Stopped");

    }

    @Test
    public void testSuccessOfTSOClientReconnectionsToARestartedTSOWithZKPublishing() throws Exception {

        // Start a TSO with ZK...
        TSOServerCommandLineConfig tsoConfig = TSOServerCommandLineConfig.configFactory(TSO_PORT, 1000);
        tsoConfig.shouldHostAndPortBePublishedInZK = true;
        tsoConfig.setLeasePeriodInMs(1000);
        injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting Initial TSO");
        tsoServer = injector.getInstance(TSOServer.class);
        tsoServer.startAndWait();
        TestUtils.waitForSocketListening(TSO_HOST, TSO_PORT, 100);
        LOG.info("Finished loading TSO");

        Thread.sleep(1500); // Allow the TSO to register

        // Then create the TSO Client under test...
        Configuration clientConf = new BaseConfiguration();
        clientConf.setProperty(TSO_ZK_CLUSTER_CONFKEY, DEFAULT_ZK_CLUSTER);

        TSOClient tsoClient = TSOClient.newBuilder().withConfiguration(clientConf).build();

        // ... and check that initially we get responses from the methods
        Long startTS = tsoClient.getNewStartTimestamp().get();
        LOG.info("Start TS {} ", startTS);
        assertEquals(startTS.longValue(), 1);

        // Then stop the server...
        tsoServer.stopAndWait();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, TSO_PORT, 1000);
        LOG.info("Initial TSO Server Stopped");

        // ... and check that we get a conn exception when trying to access the client
        try {
            startTS = tsoClient.getNewStartTimestamp().get();
            fail();
        } catch (ExecutionException e) {
            LOG.info("Exception expected");
            // Internal accessor to fsm to do the required checkings
            TSOClientImpl clientimpl = (TSOClientImpl) tsoClient;
            FsmImpl fsm = (FsmImpl) clientimpl.fsm;
            assertEquals(e.getCause().getClass(), ConnectionException.class);
            assertTrue(fsm.getState().getClass().equals(TSOClientImpl.ConnectionFailedState.class)
                    ||
                    fsm.getState().getClass().equals(TSOClientImpl.DisconnectedState.class));
        }

        // After that, simulate that a new TSO has been launched...
        Injector newInjector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Re-Starting again the TSO");
        tsoServer = newInjector.getInstance(TSOServer.class);
        tsoServer.startAndWait();
        TestUtils.waitForSocketListening(TSO_HOST, TSO_PORT, 100);
        LOG.info("Finished loading restarted TSO");

        // Finally re-check that, eventually, we can get a new value from the new TSO...
        boolean reconnectionActive = false;
        while (!reconnectionActive) {
            try {
                startTS = tsoClient.getNewStartTimestamp().get();
                reconnectionActive = true;
            } catch (ExecutionException e) {
                // Expected
            }
        }
        assertNotNull(startTS);

        // ...and stop the server
        tsoServer.stopAndWait();
        tsoServer = null;
        TestUtils.waitForSocketNotListening(TSO_HOST, TSO_PORT, 1000);
        LOG.info("Restarted TSO Server Stopped");
    }

    // **************************** Helpers ***********************************

    private static String ZK_CLUSTER = DEFAULT_ZK_CLUSTER;

    private static CuratorFramework provideInitializedZookeeperClient() throws Exception {

        LOG.info("Creating Zookeeper Client connecting to {}", ZK_CLUSTER);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder().namespace(OMID_NAMESPACE)
                .connectString(ZK_CLUSTER).retryPolicy(retryPolicy).build();

        LOG.info("Connecting to ZK cluster {}", zkClient.getState());
        zkClient.start();
        zkClient.blockUntilConnected();
        LOG.info("Connection to ZK cluster {}", zkClient.getState());

        return zkClient;
    }

}

