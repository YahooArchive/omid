package com.yahoo.omid.notifications;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

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
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.examples.Constants;
import com.yahoo.omid.notifications.client.DeltaOmid;
import com.yahoo.omid.notifications.client.IncrementalApplication;
import com.yahoo.omid.notifications.client.Observer;
import com.yahoo.omid.notifications.comm.ZNRecord;
import com.yahoo.omid.notifications.comm.ZNRecordSerializer;
import com.yahoo.omid.notifications.conf.ClientConfiguration;

public class TestDeltaOmid {

    private static Logger logger = LoggerFactory.getLogger(TestDeltaOmid.class);

    private TestingServer server;

    @Before
    public void setup() throws Exception {
        server = new TestingServer();
    }

    @After
    public void teardown() throws Exception {
        server.stop();
        server.close();
    }

    @Test
    public void testBuilderBuildsAppWithTheRigthName() throws Exception {
        Observer obs = mock(Observer.class);
        when(obs.getName()).thenReturn("TestObserver");
        ClientConfiguration appConfig = new ClientConfiguration();
        appConfig.setZkServers(server.getConnectString());
        final IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666).setConfiguration(appConfig)
                .addObserver(obs).build();

        assertEquals("This test app should be called TestApp", "TestApp", app.getName());
        app.close();
    }

    @Test
    public void testBuilderWritesTheRighNodePathForAppInZk() throws Exception {
        Observer obs = mock(Observer.class);
        when(obs.getName()).thenReturn("TestObserver");
        ClientConfiguration appConfig = new ClientConfiguration();
        appConfig.setZkServers(server.getConnectString());
        final IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666).setConfiguration(appConfig)
                .addObserver(obs).build();

        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(server.getConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        String expectedPath = ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), "TestApp");
        Stat s = zkClient.checkExists().forPath(expectedPath);
        assertTrue("The expected path for App is not found in ZK", s != null);
        app.close();
    }

    @Test
    public void testBuilderWritesTheRighNodePathForAppInstanceInZk() throws Exception {
        Observer obs = mock(Observer.class);
        when(obs.getName()).thenReturn("TestObserver");
        ClientConfiguration appConfig = new ClientConfiguration();
        appConfig.setZkServers(server.getConnectString());
        final IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666).setConfiguration(appConfig)
                .addObserver(obs).build();

        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(server.getConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        String localhost = InetAddress.getLocalHost().getHostAddress();
        String expectedPath = ZKPaths.makePath(ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), "TestApp"), localhost
                + ":" + app.getPort());
        Stat s = zkClient.checkExists().forPath(expectedPath);
        assertTrue("The expected path for App Instance is not found in ZK", s != null);
        app.close();
    }

    @Test
    public void testBuilderWritesTheRighDataInZkAppNode() throws Exception {

        final Interest interest = new Interest(Constants.TABLE_1, Constants.COLUMN_FAMILY_1, Constants.COLUMN_1);

        Observer obs = new Observer() {

            public void onColumnChanged(byte[] column, byte[] columnFamily, byte[] table, byte[] rowKey,
                    TransactionState tx) {
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
        String expectedPath = ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), "TestApp");
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
        final Interest o1i2 = new Interest(Constants.TABLE_1, Constants.COLUMN_FAMILY_1, Constants.COLUMN_2);
        final Interest o1i3 = new Interest(Constants.TABLE_1, Constants.COLUMN_FAMILY_2, Constants.COLUMN_1);
        final Interest o1i4 = new Interest(Constants.TABLE_1, Constants.COLUMN_FAMILY_2, Constants.COLUMN_2);

        final Interest o2i1 = new Interest(Constants.TABLE_1, Constants.COLUMN_FAMILY_1, Constants.COLUMN_1);
        final Interest o2i2 = new Interest(Constants.TABLE_1, Constants.COLUMN_FAMILY_1, Constants.COLUMN_2);
        final Interest o2i3 = new Interest(Constants.TABLE_2, Constants.COLUMN_FAMILY_2, Constants.COLUMN_2);
        final Interest o2i4 = new Interest(Constants.TABLE_2, Constants.COLUMN_FAMILY_2, Constants.COLUMN_3);

        Observer obs1 = mock(Observer.class);
        when(obs1.getName()).thenReturn("TestObserver3");
        List<Interest> iListObs1 = new ArrayList<Interest>();
        iListObs1.add(o1i1);
        iListObs1.add(o1i2);
        iListObs1.add(o1i3);
        iListObs1.add(o1i4);
        // when(obs1.getInterests()).thenReturn(iListObs1);
        Assert.fail();

        Observer obs2 = mock(Observer.class);
        when(obs2.getName()).thenReturn("TestObserver2");
        List<Interest> iListObs2 = new ArrayList<Interest>();
        iListObs2.add(o2i1);
        iListObs2.add(o2i2);
        iListObs2.add(o2i3);
        iListObs2.add(o2i4);
        // when(obs2.getInterests()).thenReturn(iListObs2);

        ClientConfiguration appConfig = new ClientConfiguration();
        appConfig.setZkServers(server.getConnectString());
        final IncrementalApplication app = new DeltaOmid.AppBuilder("TestApp", 6666).setConfiguration(appConfig)
                .addObserver(obs1).addObserver(obs2).build();

        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(server.getConnectString(),
                new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        String expectedPath = ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), "TestApp");
        byte[] data = zkClient.getData().forPath(expectedPath);
        ZNRecord zkRecord = (ZNRecord) new ZNRecordSerializer().deserialize(data);
        ZNRecord expectedRecord = new ZNRecord("TestApp");
        List<String> expectedObsIntList = new ArrayList();
        for (Interest i : iListObs1) {
            expectedObsIntList.add(obs1.getName() + "/" + i.toZkNodeRepresentation());
        }
        for (Interest i : iListObs2) {
            expectedObsIntList.add(obs2.getName() + "/" + i.toZkNodeRepresentation());
        }
        expectedRecord.putListField("observer-interest-list", expectedObsIntList);
        assertEquals("The content stored in the ZK record is not the same", expectedRecord, zkRecord);
        app.close();
    }

    @Test
    public void testAppInstanceNodeDissapearsInZkAfterClosingApp() throws Exception {
        Observer obs = mock(Observer.class);
        when(obs.getName()).thenReturn("TestObserver");
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
