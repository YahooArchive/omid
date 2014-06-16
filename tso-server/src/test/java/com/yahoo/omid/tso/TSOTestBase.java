package com.yahoo.omid.tso;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.BaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.InMemoryCommitTable;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.tso.util.DummyCellIdImpl;
import com.yahoo.omid.tsoclient.CellId;
import com.yahoo.omid.tsoclient.TSOClient;

public class TSOTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TSOTestBase.class);

    protected Configuration clientConf = new BaseConfiguration();
    protected TSOClient client;
    protected TSOClient client2;

    private TSOServer tso;
    private boolean tsoPaused = false;
    private volatile boolean isTsoBlockingRequest = false;
    private CommitTable commitTable = new InMemoryCommitTable();
    private CommitTable.Client commitTableClient = null;

    final static public CellId c1 = new DummyCellIdImpl(0xdeadbeefL);
    final static public CellId c2 = new DummyCellIdImpl(0xfeedcafeL);

    public void setupClient() throws Exception {

        clientConf.setProperty("tso.host", "localhost");
        clientConf.setProperty("tso.port", 1234);

        commitTableClient = commitTable.getClient().get();

        // Create the associated Handler
        client = TSOClient.newBuilder().withConfiguration(clientConf)
            .build();

        client2 = TSOClient.newBuilder().withConfiguration(clientConf)
            .build();

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

    public void teardownClient() throws Exception {
        client.close().get();
        client2.close().get();
    }

    public void pauseTSO() {
        synchronized (this) {
            tsoPaused = true;
            this.notifyAll();
        }
    }

    public void resumeTSO() {
        synchronized (this) {
            tsoPaused = false;
            this.notifyAll();
        }
    }

    public boolean isTsoBlockingRequest() {
        return isTsoBlockingRequest;
    }

    @Before
    public void setupTSO() throws Exception {
        LOG.info("Starting TSO");
        MetricRegistry metrics = new MetricRegistry();
        TimestampOracle timestampOracle = new TimestampOracle(metrics, new TimestampOracle.InMemoryTimestampStorage()) {
            @Override
            public long next() throws IOException {
                while(tsoPaused) {
                    isTsoBlockingRequest = true;
                    synchronized (TSOTestBase.this) {
                        try {
                            TSOTestBase.this.wait();
                        } catch (InterruptedException e) {
                            LOG.error("Interrupted whilst paused");
                            Thread.currentThread().interrupt();
                        }
                    }
                }
                isTsoBlockingRequest = false;
                return super.next();
            }
        };
        tso = new TSOServer(TSOServerConfig.configFactory(1234, 1000), metrics,
                            commitTable, timestampOracle);
        tso.startAndWait();
        TestUtils.waitForSocketListening("localhost", 1234, 100);
        LOG.info("Finished loading TSO");

        Thread.currentThread().setName("JUnit Thread");

        setupClient();
    }

    @After
    public void teardownTSO() throws Exception {

        teardownClient();

        tso.stopAndWait();

        tso = null;

        TestUtils.waitForSocketNotListening("localhost", 1234, 1000);

    }

    protected boolean recoveryEnabled() {
        return false;
    }

}