/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.tso;

import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.omid.TestUtils;
import org.apache.omid.tso.TSOStateManager.TSOState;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.apache.omid.tso.client.TSOClient.DEFAULT_ZK_CLUSTER;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class TestLeaseManager {

    private static final long DUMMY_EPOCH_1 = 1L;
    private static final long DUMMY_EPOCH_2 = 2L;
    private static final long DUMMY_EPOCH_3 = 3L;
    private static final long DUMMY_LOW_WATERMARK_1 = DUMMY_EPOCH_1;
    private static final long DUMMY_LOW_WATERMARK_2 = DUMMY_EPOCH_2;
    private static final long DUMMY_LOW_WATERMARK_3 = DUMMY_EPOCH_3;

    private static final String LEASE_MGR_ID_1 = "LM1";
    private static final String LEASE_MGR_ID_2 = "LM2";
    private static final String INSTANCE_ID_1 = "LM1" + "#";
    private static final String INSTANCE_ID_2 = "LM2" + "#";

    private static final Logger LOG = LoggerFactory.getLogger(TestLeaseManager.class);

    private static final long TEST_LEASE_PERIOD_IN_MS = 1000; // 1 second

    private CuratorFramework zkClient;
    private TestingServer zkServer;

    @Mock
    private Panicker panicker;

    private PausableLeaseManager leaseManager1;
    private PausableLeaseManager leaseManager2;

    @BeforeClass
    public void beforeClass() throws Exception {

        LOG.info("Starting ZK Server");
        zkServer = TestUtils.provideTestingZKServer();
        LOG.info("ZK Server Started @ {}", zkServer.getConnectString());

        zkClient = TestUtils.provideConnectedZKClient(DEFAULT_ZK_CLUSTER);

    }

    @AfterClass
    public void afterClass() throws Exception {

        zkClient.close();

        CloseableUtils.closeQuietly(zkServer);
        zkServer = null;
        LOG.info("ZK Server Stopped");

    }

    @Test(timeOut = 30_000)
    public void testErrorInitializingTSOStateExitsTheTSO() throws Exception {

        final String TEST_TSO_LEASE_PATH = "/test0_tsolease";
        final String TEST_CURRENT_TSO_PATH = "/test0_currenttso";

        Panicker panicker = spy(new MockPanicker());

        TSOChannelHandler tsoChannelHandler = mock(TSOChannelHandler.class);
        TSOStateManager stateManager = mock(TSOStateManager.class);
        when(stateManager.initialize()).thenThrow(new IOException());
        leaseManager1 = new PausableLeaseManager(LEASE_MGR_ID_1,
                                                 tsoChannelHandler,
                                                 stateManager,
                                                 TEST_LEASE_PERIOD_IN_MS,
                                                 TEST_TSO_LEASE_PATH,
                                                 TEST_CURRENT_TSO_PATH,
                                                 zkClient,
                                                 panicker);
        leaseManager1.startService();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        verify(panicker, timeout(2000).atLeastOnce()).panic(anyString(), any(IOException.class));

        leaseManager1.stopService();

    }

    @Test(timeOut = 60000)
    public void testLeaseHolderDoesNotChangeWhenPausedForALongTimeAndTheresNoOtherInstance() throws Exception {

        final String TEST_TSO_LEASE_PATH = "/test1_tsolease";
        final String TEST_CURRENT_TSO_PATH = "/test1_currenttso";

        // Launch the instance under test...
        TSOChannelHandler tsoChannelHandler1 = mock(TSOChannelHandler.class);
        TSOStateManager stateManager1 = mock(TSOStateManager.class);
        when(stateManager1.initialize()).thenReturn(new TSOState(DUMMY_LOW_WATERMARK_1, DUMMY_EPOCH_1));
        leaseManager1 = new PausableLeaseManager(LEASE_MGR_ID_1,
                                                 tsoChannelHandler1,
                                                 stateManager1,
                                                 TEST_LEASE_PERIOD_IN_MS,
                                                 TEST_TSO_LEASE_PATH,
                                                 TEST_CURRENT_TSO_PATH,
                                                 zkClient,
                                                 panicker);
        leaseManager1.startService();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... check is the lease holder
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1 + "1");
        assertTrue(leaseManager1.stillInLeasePeriod());

        // Then, pause instance when trying to renew lease...
        leaseManager1.pausedInTryToRenewLeasePeriod();

        // ...let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ...check that nothing changed...
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1 + "1");

        // Finally, resume the instance...
        leaseManager1.resume();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... and check again that nothing changed
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1 + "1");
        assertFalse(leaseManager1.stillInLeasePeriod()); // Must not be master as it should have triggered and exception

    }

    @Test(timeOut = 60_000)
    public void testLeaseHolderDoesNotChangeWhenANewLeaseManagerIsUp() throws Exception {

        final String TEST_TSO_LEASE_PATH = "/test2_tsolease";
        final String TEST_CURRENT_TSO_PATH = "/test2_currenttso";

        // Launch the master instance...
        TSOChannelHandler tsoChannelHandler1 = mock(TSOChannelHandler.class);
        TSOStateManager stateManager1 = mock(TSOStateManager.class);
        when(stateManager1.initialize()).thenReturn(new TSOState(DUMMY_LOW_WATERMARK_1, DUMMY_EPOCH_1));
        leaseManager1 = new PausableLeaseManager(LEASE_MGR_ID_1,
                                                 tsoChannelHandler1,
                                                 stateManager1,
                                                 TEST_LEASE_PERIOD_IN_MS,
                                                 TEST_TSO_LEASE_PATH,
                                                 TEST_CURRENT_TSO_PATH,
                                                 zkClient,
                                                 panicker);

        leaseManager1.startService();

        // ...let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ...so it should be the current holder of the lease
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1 + "1");
        assertTrue(leaseManager1.stillInLeasePeriod());

        // Then launch another instance...
        TSOChannelHandler tsoChannelHandler2 = mock(TSOChannelHandler.class);
        TSOStateManager stateManager2 = mock(TSOStateManager.class);
        when(stateManager2.initialize()).thenReturn(new TSOState(DUMMY_LOW_WATERMARK_2, DUMMY_EPOCH_2));
        leaseManager2 = new PausableLeaseManager(LEASE_MGR_ID_2,
                                                 tsoChannelHandler2,
                                                 stateManager2,
                                                 TEST_LEASE_PERIOD_IN_MS,
                                                 TEST_TSO_LEASE_PATH,
                                                 TEST_CURRENT_TSO_PATH,
                                                 zkClient,
                                                 panicker);
        leaseManager2.startService();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... and after the period, the first instance should be still the holder
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1 + "1");
        assertTrue(leaseManager1.stillInLeasePeriod());
        assertFalse(leaseManager2.stillInLeasePeriod());
    }

    @Test(timeOut = 60_000)
    public void testLeaseHolderChangesWhenActiveLeaseManagerIsPaused() throws Exception {

        final String TEST_TSO_LEASE_PATH = "/test3_tsolease";
        final String TEST_CURRENT_TSO_PATH = "/test3_currenttso";

        // Launch the master instance...
        TSOChannelHandler tsoChannelHandler1 = mock(TSOChannelHandler.class);
        TSOStateManager stateManager1 = mock(TSOStateManager.class);
        when(stateManager1.initialize()).thenReturn(new TSOState(DUMMY_LOW_WATERMARK_1, DUMMY_EPOCH_1));
        leaseManager1 = new PausableLeaseManager(LEASE_MGR_ID_1,
                                                 tsoChannelHandler1,
                                                 stateManager1,
                                                 TEST_LEASE_PERIOD_IN_MS,
                                                 TEST_TSO_LEASE_PATH,
                                                 TEST_CURRENT_TSO_PATH,
                                                 zkClient,
                                                 panicker);

        leaseManager1.startService();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... so it should be the current holder of the lease
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1 + "1");
        assertTrue(leaseManager1.stillInLeasePeriod());

        // Then launch another instance...
        TSOChannelHandler tsoChannelHandler2 = mock(TSOChannelHandler.class);
        TSOStateManager stateManager2 = mock(TSOStateManager.class);
        when(stateManager2.initialize()).thenReturn(new TSOState(DUMMY_LOW_WATERMARK_2, DUMMY_EPOCH_2));
        leaseManager2 = new PausableLeaseManager(LEASE_MGR_ID_2,
                                                 tsoChannelHandler2,
                                                 stateManager2,
                                                 TEST_LEASE_PERIOD_IN_MS,
                                                 TEST_TSO_LEASE_PATH,
                                                 TEST_CURRENT_TSO_PATH,
                                                 zkClient,
                                                 panicker);
        leaseManager2.startService();

        // ... and pause active lease manager...
        leaseManager1.pausedInStillInLeasePeriod();

        // ... and let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... and check that lease owner should have changed to the second instance
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_2);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_2 + "2");
        assertTrue(leaseManager2.stillInLeasePeriod());

        // Now, lets resume the first instance...
        when(stateManager1.initialize()).thenReturn(new TSOState(DUMMY_LOW_WATERMARK_3, DUMMY_EPOCH_3));
        leaseManager1.resume();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // and check the lease owner is still the second instance (preserves the lease)
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_2);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_2 + "2");
        assertFalse(leaseManager1.stillInLeasePeriod());
        assertTrue(leaseManager2.stillInLeasePeriod());

        // Finally, pause active lease manager when trying to renew lease...
        leaseManager2.pausedInTryToRenewLeasePeriod();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... and check lease owner is has changed again to the first instance
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1 + "3");
        assertFalse(leaseManager2.stillInLeasePeriod());
        assertTrue(leaseManager1.stillInLeasePeriod());

        // Resume the second instance...
        leaseManager2.resume();

        // ... let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        // ... but the lease owner should still be the first instance
        checkLeaseHolder(TEST_TSO_LEASE_PATH, LEASE_MGR_ID_1);
        checkInstanceId(TEST_CURRENT_TSO_PATH, INSTANCE_ID_1 + "3");
        assertFalse(leaseManager2.stillInLeasePeriod());
        assertTrue(leaseManager1.stillInLeasePeriod());

    }


    @Test(timeOut = 40_000)
    public void testLeaseManagerPanicsWhenUnexpectedInfoIsFoundInCurrentTSOZnode() throws Exception {

        final String TEST_TSO_LEASE_PATH = "/test_wronginfo_tsolease";
        final String TEST_CURRENT_TSO_PATH = "/test_wronginfo_currenttso";

        Panicker panicker = spy(new MockPanicker());

        // Launch the master instance...
        TSOStateManager stateManager1 = mock(TSOStateManager.class);
        when(stateManager1.initialize()).thenReturn(new TSOState(DUMMY_LOW_WATERMARK_1, DUMMY_EPOCH_1));
        PausableLeaseManager leaseManager = new PausableLeaseManager(LEASE_MGR_ID_1,
                                                                     mock(TSOChannelHandler.class),
                                                                     stateManager1,
                                                                     TEST_LEASE_PERIOD_IN_MS,
                                                                     TEST_TSO_LEASE_PATH,
                                                                     TEST_CURRENT_TSO_PATH,
                                                                     zkClient,
                                                                     panicker);

        leaseManager.startService();
        // ...and let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        leaseManager.pausedInTryToRenewLeasePeriod();

        // 1st Panic test) Inject corrupted data in the ZNode, force reelection and test the panicker is exercised
        zkClient.setData().forPath(TEST_CURRENT_TSO_PATH, "CorruptedData!!!".getBytes());

        // ...and let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);
        leaseManager.resume();
        // ...and let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        ArgumentCaptor<IllegalArgumentException> trowableIAE = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(panicker, times(2)).panic(anyString(), trowableIAE.capture());
        assertTrue(trowableIAE.getValue() instanceof IllegalArgumentException);
        assertTrue(trowableIAE.getValue().getMessage().contains("Incorrect TSO Info found"));

        // 2nd Panic test) Simulate that a new master appeared in the meantime, force reelection
        // and test the panicker is exercised
        reset(panicker);
        zkClient.setData().forPath(TEST_CURRENT_TSO_PATH, "newTSO:12345#10000".getBytes());

        leaseManager.pausedInTryToRenewLeasePeriod();

        // ...and let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);
        leaseManager.resume();
        // ...and let the test run for some time...
        Thread.sleep(TEST_LEASE_PERIOD_IN_MS * 2);

        ArgumentCaptor<LeaseManagement.LeaseManagementException> trowableLME =
                ArgumentCaptor.forClass(LeaseManagement.LeaseManagementException.class);
        verify(panicker, times(2)).panic(anyString(), trowableLME.capture());
        assertTrue(trowableLME.getValue() instanceof LeaseManagement.LeaseManagementException);
        assertTrue(trowableLME.getValue().getMessage().contains("Another TSO replica was found"));
    }

    @Test(timeOut = 1000)
    public void testNonHALeaseManager() throws Exception {

        // Launch the instance...
        VoidLeaseManager leaseManager = new VoidLeaseManager(mock(TSOChannelHandler.class),
                                                             mock(TSOStateManager.class));

        leaseManager.startService();
        assertTrue(leaseManager.stillInLeasePeriod());
        leaseManager.stopService();

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Checkers
    // ----------------------------------------------------------------------------------------------------------------

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

}
