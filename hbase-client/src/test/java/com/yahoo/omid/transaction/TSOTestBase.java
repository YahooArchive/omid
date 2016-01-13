package com.yahoo.omid.transaction;

import com.yahoo.omid.tso.PausableTimestampOracle;
import com.yahoo.omid.tso.TSOMockModule;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import com.yahoo.omid.tso.TimestampOracle;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.tso.util.DummyCellIdImpl;
import com.yahoo.omid.tsoclient.CellId;
import com.yahoo.omid.tsoclient.TSOClient;

public class TSOTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TSOTestBase.class);

    private Injector injector = null;

    protected Configuration clientConf = new BaseConfiguration();
    protected TSOClient client;
    protected TSOClient client2;

    private PausableTimestampOracle pausableTSOracle;
    private TSOServer tso;

    private CommitTable commitTable;
    private CommitTable.Client commitTableClient = null;

    final static public CellId c1 = new DummyCellIdImpl(0xdeadbeefL);
    final static public CellId c2 = new DummyCellIdImpl(0xfeedcafeL);

    @BeforeMethod
    public void setupTSO() throws Exception {
        injector = Guice.createInjector(new TSOMockModule(TSOServerCommandLineConfig.configFactory(1234, 1000)));
        LOG.info("Starting TSO");
        pausableTSOracle = (PausableTimestampOracle) injector.getInstance(TimestampOracle.class);
        tso = injector.getInstance(TSOServer.class);
        tso.startAndWait();
        TestUtils.waitForSocketListening("localhost", 1234, 100);
        LOG.info("Finished loading TSO");

        Thread.currentThread().setName("JUnit Thread");

        setupClient();
    }

    private void setupClient() throws Exception {

        clientConf.setProperty("tso.host", "localhost");
        clientConf.setProperty("tso.port", 1234);

        commitTable = injector.getInstance(CommitTable.class);
        commitTableClient = commitTable.getClient().get();
        
        // Create the associated Handler
        client = TSOClient.newBuilder().withConfiguration(clientConf)
                .build();

        client2 = TSOClient.newBuilder().withConfiguration(clientConf)
                .build();

    }

    @AfterMethod
    public void teardownTSO() throws Exception {

        teardownClient();

        tso.stopAndWait();

        tso = null;

        TestUtils.waitForSocketNotListening("localhost", 1234, 1000);

    }

    private void teardownClient() throws Exception {
        client.close().get();
        client2.close().get();
    }

    public TSOClient getClient() {
        return client;
    }

    public Configuration getClientConfiguration() {
        return clientConf;
    }

    public CommitTable getCommitTable() {
        return commitTable;
    }

    public CommitTable.Client getCommitTableClient() {
        return commitTableClient;
    }

    public void pauseTSO() {
        pausableTSOracle.pause();
    }

    public void resumeTSO() {
        pausableTSOracle.resume();
    }

    public boolean isTsoBlockingRequest() {
        return pausableTSOracle.isTSOPaused();
    }

}
