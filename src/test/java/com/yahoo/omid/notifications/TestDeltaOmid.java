package com.yahoo.omid.notifications;

import static com.yahoo.omid.examples.Constants.COLUMN_1;
import static com.yahoo.omid.examples.Constants.COLUMN_FAMILY_1;
import static com.yahoo.omid.examples.Constants.TABLE_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.client.Result;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingServer;
import com.netflix.curator.utils.ZKPaths;
import com.yahoo.omid.examples.Constants;
import com.yahoo.omid.notifications.client.DeltaOmid;
import com.yahoo.omid.notifications.client.IncrementalApplication;
import com.yahoo.omid.notifications.client.Observer;
import com.yahoo.omid.notifications.comm.ZNRecord;
import com.yahoo.omid.notifications.comm.ZNRecordSerializer;
import com.yahoo.omid.notifications.conf.ClientConfiguration;
import com.yahoo.omid.transaction.Transaction;

public class TestDeltaOmid extends TestInfrastructure {

    private static Logger logger = LoggerFactory.getLogger(TestDeltaOmid.class);

    private TestingServer server;

    @Before
    public void setup() throws Exception {
        server = new TestingServer();
    }

    @After
    public void teardown() throws Exception {
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(server.getConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        zkClient.delete().forPath(ZkTreeUtils.getAppsNodePath());

        server.stop();
        server.close();
    }

    @Test
    public void testBuilderBuildsAppWithTheRigthName() throws Exception {
        Interest interest = new Interest(TABLE_1, COLUMN_FAMILY_1, COLUMN_1);
        Observer obs = mock(Observer.class);
        when(obs.getName()).thenReturn("TestObserver");
        when(obs.getInterest()).thenReturn(interest);
        ClientConfiguration appConfig = new ClientConfiguration();
        appConfig.setZkServers(server.getConnectString());
        final IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666).setConfiguration(appConfig)
                .addObserver(obs).build();

        assertEquals("This test app should be called TestApp", "TestApp", app.getName());
        app.close();
    }

    @Test
    public void testBuilderWritesTheRighNodePathForAppInZk() throws Exception {
        Interest interest = new Interest(TABLE_1, COLUMN_FAMILY_1, COLUMN_1);
        Observer obs = mock(Observer.class);
        when(obs.getName()).thenReturn("TestObserver");
        when(obs.getInterest()).thenReturn(interest);
        ClientConfiguration appConfig = new ClientConfiguration();
        appConfig.setZkServers(server.getConnectString());
        final IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666).setConfiguration(appConfig)
                .addObserver(obs).build();

        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(server.getConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        String expectedPath = ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), "TestApp0000000000");
        Stat s = zkClient.checkExists().forPath(expectedPath);
        assertTrue("The expected path for App is not found in ZK", s != null);
        app.close();
    }

    @Test
    public void testBuilderWritesTheRighNodePathForAppInstanceInZk() throws Exception {
        Interest interest = new Interest(TABLE_1, COLUMN_FAMILY_1, COLUMN_1);
        Observer obs = mock(Observer.class);
        when(obs.getName()).thenReturn("TestObserver");
        when(obs.getInterest()).thenReturn(interest);
        ClientConfiguration appConfig = new ClientConfiguration();
        appConfig.setZkServers(server.getConnectString());
        final IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666).setConfiguration(appConfig)
                .addObserver(obs).build();

        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(server.getConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        String expectedPath = ZkTreeUtils.getAppsNodePath();
        List<String> children = zkClient.getChildren().forPath(expectedPath);
        assertNotNull("Returned children list is null", children);
        assertFalse("No app instance registered for TestApp", children.isEmpty());
        assertEquals("More than one instances registered for TestApp", 1, children.size());
        app.close();
    }

    @Test
    public void testBuilderWritesTheRighDataInZkAppNode() throws Exception {

        final Interest interest = new Interest(TABLE_1, COLUMN_FAMILY_1, COLUMN_1);

        Observer obs = new Observer() {

            public void onInterestChanged(Result rowData, Transaction tx) {
                logger.info("I'm observer " + getName());
            }

            @Override
            public String getName() {
                return "TestObserver";
            }

            @Override
            public Interest getInterest() {
                return interest;
            }
        };

        ClientConfiguration appConfig = new ClientConfiguration();
        appConfig.setZkServers(server.getConnectString());
        final IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666).setConfiguration(appConfig)
                .addObserver(obs).build();

        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(server.getConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        String expectedPath = ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), "TestApp0000000000");
        byte[] data = zkClient.getData().forPath(expectedPath);
        ZNRecord zkRecord = (ZNRecord) new ZNRecordSerializer().deserialize(data);
        ZNRecord expectedRecord = new ZNRecord("TestApp");
        expectedRecord.putListField("observer-interest-list",
                Collections.singletonList("TestObserver" + "/" + interest.toZkNodeRepresentation()));
        assertEquals("The content stored in the ZK record is not the same", expectedRecord, zkRecord);
        app.close();
    }

    @Test
    public void testBuilderWritesTheRighComplexDataInZkAppNodeForTwoObservers() throws Exception {

        final Interest o1i1 = new Interest(Constants.TABLE_1, Constants.COLUMN_FAMILY_1, Constants.COLUMN_1);
        final Interest o2i1 = new Interest(Constants.TABLE_1, Constants.COLUMN_FAMILY_1, Constants.COLUMN_2);

        Observer obs1 = mock(Observer.class);
        when(obs1.getName()).thenReturn("TestObserver3");
        when(obs1.getInterest()).thenReturn(o1i1);

        Observer obs2 = mock(Observer.class);
        when(obs2.getName()).thenReturn("TestObserver2");
        when(obs2.getInterest()).thenReturn(o2i1);

        ClientConfiguration appConfig = new ClientConfiguration();
        appConfig.setZkServers(server.getConnectString());
        final IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666).setConfiguration(appConfig)
                .addObserver(obs1).addObserver(obs2).build();

        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(server.getConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        String expectedPath = ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), "TestApp0000000000");
        byte[] data = zkClient.getData().forPath(expectedPath);
        ZNRecord zkRecord = (ZNRecord) new ZNRecordSerializer().deserialize(data);
        ZNRecord expectedRecord = new ZNRecord("TestApp");
        List<String> expectedObsIntList = new ArrayList<String>();
        expectedObsIntList.add(obs1.getName() + "/" + o1i1.toZkNodeRepresentation());
        expectedObsIntList.add(obs2.getName() + "/" + o2i1.toZkNodeRepresentation());
        expectedRecord.putListField("observer-interest-list", expectedObsIntList);
        assertEquals("The content stored in the ZK record is not the same", expectedRecord, zkRecord);
        app.close();
    }

    @Test
    public void testAppInstanceNodeDissapearsInZkAfterClosingApp() throws Exception {
        Interest interest = new Interest(TABLE_1, COLUMN_FAMILY_1, COLUMN_1);
        Observer obs = mock(Observer.class);
        when(obs.getName()).thenReturn("TestObserver");
        when(obs.getInterest()).thenReturn(interest);
        ClientConfiguration appConfig = new ClientConfiguration();
        appConfig.setZkServers(server.getConnectString());
        final IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666).setConfiguration(appConfig)
                .addObserver(obs).build();

        app.close();
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(server.getConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        String localhost = InetAddress.getLocalHost().getHostAddress();
        String expectedPath = ZKPaths.makePath(ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), "TestApp"), localhost);
        Stat s = zkClient.checkExists().forPath(expectedPath);
        assertTrue("The expected path for App Instance should not be found in ZK", s == null);
    }

}
