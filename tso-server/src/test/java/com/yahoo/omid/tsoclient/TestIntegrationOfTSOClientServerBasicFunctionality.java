package com.yahoo.omid.tsoclient;

import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.tso.TSOMockModule;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import com.yahoo.omid.tso.util.DummyCellIdImpl;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

public class TestIntegrationOfTSOClientServerBasicFunctionality {

    private static final Logger LOG = LoggerFactory.getLogger(TestIntegrationOfTSOClientServerBasicFunctionality.class);

    private final int TSO_SERVER_PORT = 1234;

    // Cells for tests
    final static public CellId c1 = new DummyCellIdImpl(0xdeadbeefL);
    final static public CellId c2 = new DummyCellIdImpl(0xfeedcafeL);

    // Required infrastructure for TSO client-server integration testing
    private TSOServer tso;
    private TSOClient client;
    private CommitTable.Client commitTableClient;

    @BeforeClass
    public void setup() throws Exception {

        Module tsoServerMockModule = new TSOMockModule(TSOServerCommandLineConfig.configFactory(TSO_SERVER_PORT, 1000));
        Injector injector = Guice.createInjector(tsoServerMockModule);

        CommitTable commitTable = injector.getInstance(CommitTable.class);
        commitTableClient = commitTable.getClient().get();

        LOG.info("==================================================================================================");
        LOG.info("======================================= Init TSO Server ==========================================");
        LOG.info("==================================================================================================");

        tso = injector.getInstance(TSOServer.class);
        tso.startAndWait();
        TestUtils.waitForSocketListening("localhost", TSO_SERVER_PORT, 100);

        LOG.info("==================================================================================================");
        LOG.info("===================================== TSO Server Initialized =====================================");
        LOG.info("==================================================================================================");

        LOG.info("==================================================================================================");
        LOG.info("======================================= Setup TSO Clients ========================================");
        LOG.info("==================================================================================================");

        Configuration clientConf = new BaseConfiguration();
        clientConf.setProperty("tso.host", "localhost");
        clientConf.setProperty("tso.port", TSO_SERVER_PORT);
        client = TSOClient.newBuilder().withConfiguration(clientConf).build();

        LOG.info("==================================================================================================");
        LOG.info("===================================== TSO Clients Initialized ====================================");
        LOG.info("==================================================================================================");

        Thread.currentThread().setName("Test Thread");

    }

    @AfterClass
    public void tearDown() throws Exception {

        client.close().get();

        tso.stopAndWait();
        tso = null;
        TestUtils.waitForSocketNotListening("localhost", TSO_SERVER_PORT, 1000);

    }

    @Test(timeOut = 10_000)
    public void testTimestampsOrderingGrowMonotonically() throws Exception {
        long referenceTimestamp;
        long startTsTx1 = client.getNewStartTimestamp().get();
        referenceTimestamp = startTsTx1;

        long startTsTx2 = client.getNewStartTimestamp().get();
        assertEquals(startTsTx2, ++referenceTimestamp, "Should grow monotonically");
        assertTrue(startTsTx2 > startTsTx1, "Two timestamps obtained consecutively should grow");

        long commitTsTx2 = client.commit(startTsTx2, Sets.newHashSet(c1)).get();
        assertEquals(commitTsTx2, ++referenceTimestamp, "Should grow monotonically");

        long commitTsTx1 = client.commit(startTsTx1, Sets.newHashSet(c2)).get();
        assertEquals(commitTsTx1, ++referenceTimestamp, "Should grow monotonically");

        long startTsTx3 = client.getNewStartTimestamp().get();
        assertEquals(startTsTx3, ++referenceTimestamp, "Should grow monotonically");
    }

    @Test(timeOut = 10_000)
    public void testSimpleTransactionWithNoWriteSetCanCommit() throws Exception {
        long startTsTx1 = client.getNewStartTimestamp().get();
        long commitTsTx1 = client.commit(startTsTx1, Sets.<CellId>newHashSet()).get();
        assertTrue(commitTsTx1 > startTsTx1);
    }

    @Test(timeOut = 10_000)
    public void testTransactionWithMassiveWriteSetCanCommit() throws Exception {
        long startTs = client.getNewStartTimestamp().get();

        Set<CellId> cells = new HashSet<>();
        for (int i = 0; i < 1_000_000; i++) {
            cells.add(new DummyCellIdImpl(i));
        }

        long commitTs = client.commit(startTs, cells).get();
        assertTrue(commitTs > startTs, "Commit TS should be higher than Start TS");
    }

    @Test(timeOut = 10_000)
    public void testMultipleSerialCommitsDoNotConflict() throws Exception {
        long startTsTx1 = client.getNewStartTimestamp().get();
        long commitTsTx1 = client.commit(startTsTx1, Sets.newHashSet(c1)).get();
        assertTrue(commitTsTx1 > startTsTx1, "Commit TS must be greater than Start TS");

        long startTsTx2 = client.getNewStartTimestamp().get();
        assertTrue(startTsTx2 > commitTsTx1, "TS should grow monotonically");

        long commitTsTx2 = client.commit(startTsTx2, Sets.newHashSet(c1, c2)).get();
        assertTrue(commitTsTx2 > startTsTx2, "Commit TS must be greater than Start TS");

        long startTsTx3 = client.getNewStartTimestamp().get();
        long commitTsTx3 = client.commit(startTsTx3, Sets.newHashSet(c2)).get();
        assertTrue(commitTsTx3 > startTsTx3, "Commit TS must be greater than Start TS");
    }

    @Test(timeOut = 10_000)
    public void testCommitWritesToCommitTable() throws Exception {
        long startTsForTx1 = client.getNewStartTimestamp().get();
        long startTsForTx2 = client.getNewStartTimestamp().get();
        assertTrue(startTsForTx2 > startTsForTx1, "Start TS should grow");

        assertFalse(commitTableClient.getCommitTimestamp(startTsForTx1).get().isPresent(),
                "Commit TS for Tx1 shouldn't appear in Commit Table");

        long commitTsForTx1 = client.commit(startTsForTx1, Sets.newHashSet(c1)).get();
        assertTrue(commitTsForTx1 > startTsForTx1, "Commit TS should be higher than Start TS for the same tx");

        Long commitTs1InCommitTable = commitTableClient.getCommitTimestamp(startTsForTx1).get().get().getValue();
        assertNotNull("Tx is committed, should return as such from Commit Table", commitTs1InCommitTable);
        assertEquals(commitTsForTx1, (long) commitTs1InCommitTable,
                "getCommitTimestamp() & commit() should report same Commit TS value for same tx");
        assertTrue(commitTs1InCommitTable > startTsForTx2, "Commit TS should be higher than tx's Start TS");
    }

    @Test(timeOut = 10_000)
    public void testTwoConcurrentTxWithOverlappingWritesetsHaveConflicts() throws Exception {
        long startTsTx1 = client.getNewStartTimestamp().get();
        long startTsTx2 = client.getNewStartTimestamp().get();
        assertTrue(startTsTx2 > startTsTx1, "Second TX should have higher TS");

        long commitTsTx1 = client.commit(startTsTx1, Sets.newHashSet(c1)).get();
        assertTrue(commitTsTx1 > startTsTx1, "Commit TS must be higher than Start TS for the same tx");

        try {
            client.commit(startTsTx2, Sets.newHashSet(c1, c2)).get();
            Assert.fail("Second TX should fail on commit");
        } catch (ExecutionException ee) {
            assertEquals(TSOClient.AbortException.class, ee.getCause().getClass(), "Should have aborted");
        }
    }

}
