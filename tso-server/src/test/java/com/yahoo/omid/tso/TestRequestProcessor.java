package com.yahoo.omid.tso;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.List;

import org.jboss.netty.channel.Channel;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;

public class TestRequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TestRequestProcessor.class);
    private MetricsRegistry metrics = new NullMetricsProvider();

    private PersistenceProcessor persist;

    private TSOStateManager stateManager;

    // Request processor under test
    private RequestProcessor requestProc;

    @BeforeMethod
    public void beforeMethod() throws Exception {

        // Build the required scaffolding for the test
        MetricsRegistry metrics = new NullMetricsProvider();

        TimestampOracleImpl timestampOracle = new TimestampOracleImpl(metrics,
                new TimestampOracleImpl.InMemoryTimestampStorage(), new MockPanicker());

        stateManager = new TSOStateManagerImpl(timestampOracle);

        persist = mock(PersistenceProcessor.class);

        TSOServerConfig config = new TSOServerConfig();
        config.setMaxItems(1000);

        requestProc = new RequestProcessorImpl(metrics,
                                               timestampOracle,
                                               persist,
                                               new MockPanicker(),
                                               config);

        // Initialize the state for the experiment
        stateManager.register(requestProc);
        stateManager.reset();
    }

    @Test(timeOut = 30000)
    public void testTimestamp() throws Exception {
        requestProc.timestampRequest(null, new MonitoringContext(metrics));
        ArgumentCaptor<Long> firstTScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(1)).persistTimestamp(
                firstTScapture.capture(), any(Channel.class), any(MonitoringContext.class));

        long firstTS = firstTScapture.getValue();
        // verify that timestamps increase monotonically
        for (int i = 0; i < 100; i++) {
            requestProc.timestampRequest(null, new MonitoringContext(metrics));
            verify(persist, timeout(100).times(1)).persistTimestamp(eq(firstTS++), any(Channel.class), any(MonitoringContext.class));
        }
    }

    @Test(timeOut = 30000)
    public void testCommit() throws Exception {

        requestProc.timestampRequest(null, new MonitoringContext(metrics));
        ArgumentCaptor<Long> TScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(1)).persistTimestamp(
                TScapture.capture(), any(Channel.class), any(MonitoringContext.class));
        long firstTS = TScapture.getValue();

        List<Long> writeSet = Lists.newArrayList(1L, 20L, 203L);
        requestProc.commitRequest(firstTS - 1, writeSet, false, null, new MonitoringContext(metrics));
        verify(persist, timeout(100).times(1)).persistAbort(eq(firstTS - 1), anyBoolean(), any(Channel.class), any(MonitoringContext.class));

        requestProc.commitRequest(firstTS, writeSet, false, null, new MonitoringContext(metrics));
        ArgumentCaptor<Long> commitTScapture = ArgumentCaptor.forClass(Long.class);

        verify(persist, timeout(100).times(1)).persistCommit(eq(firstTS), commitTScapture.capture(), any(Channel.class), any(MonitoringContext.class));
        assertTrue("Commit TS must be greater than start TS", commitTScapture.getValue() > firstTS);

        // test conflict
        requestProc.timestampRequest(null, new MonitoringContext(metrics));
        TScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(2)).persistTimestamp(
                TScapture.capture(), any(Channel.class), any(MonitoringContext.class));
        long secondTS = TScapture.getValue();

        requestProc.timestampRequest(null, new MonitoringContext(metrics));
        TScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(3)).persistTimestamp(
                TScapture.capture(), any(Channel.class), any(MonitoringContext.class));
        long thirdTS = TScapture.getValue();

        requestProc.commitRequest(thirdTS, writeSet, false, null, new MonitoringContext(metrics));
        verify(persist, timeout(100).times(1)).persistCommit(eq(thirdTS), anyLong(), any(Channel.class), any(MonitoringContext.class));
        requestProc.commitRequest(secondTS, writeSet, false, null, new MonitoringContext(metrics));
        verify(persist, timeout(100).times(1)).persistAbort(eq(secondTS), anyBoolean(), any(Channel.class), any(MonitoringContext.class));
    }

    @Test(timeOut = 30000)
    public void testCommitRequestAbortsWhenResettingRequestProcessorState() throws Exception {

        List<Long> writeSet = Collections.<Long> emptyList();

        // Start a transaction...
        requestProc.timestampRequest(null, new MonitoringContext(metrics));
        ArgumentCaptor<Long> capturedTS = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(1)).persistTimestamp(capturedTS.capture(),
                                                                any(Channel.class),
                                                                any(MonitoringContext.class));
        long startTS = capturedTS.getValue();

        // ... simulate the reset of the RequestProcessor state (e.g. due to
        // a change in mastership) and...
        stateManager.reset();

        // ...check that the transaction is aborted when trying to commit
        requestProc.commitRequest(startTS, writeSet, false, null, new MonitoringContext(metrics));
        verify(persist, timeout(100).times(1)).persistAbort(eq(startTS), anyBoolean(), any(Channel.class), any(MonitoringContext.class));

    }

}