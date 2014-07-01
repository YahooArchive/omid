package com.yahoo.omid.tso;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.jboss.netty.channel.Channel;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

public class TestRequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TestRequestProcessor.class);

    private MetricRegistry metrics = new MetricRegistry();

    @Test(timeout=10000)
    public void testTimestamp() throws Exception {
        PersistenceProcessor persist = mock(PersistenceProcessor.class);
        TimestampOracleImpl timestampOracle = new TimestampOracleImpl(metrics,
                new TimestampOracleImpl.InMemoryTimestampStorage(), new MockPanicker());
        TSOServerConfig config = new TSOServerConfig();
        config.setMaxItems(1000);
        RequestProcessor proc = new RequestProcessorImpl(metrics, timestampOracle,
                                                         persist, new MockPanicker(), config);

        proc.timestampRequest(null);
        ArgumentCaptor<Long> firstTScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(1)).persistTimestamp(
                firstTScapture.capture(), any(Channel.class));

        long firstTS = firstTScapture.getValue();
        // verify that timestamps increase monotonically
        for (int i = 0; i < 100; i++) {
            proc.timestampRequest(null);
            verify(persist, timeout(100).times(1)).persistTimestamp(eq(firstTS++), any(Channel.class));
        }
    }

    @Test(timeout=10000)
    public void testCommit() throws Exception {
        List<Long> rows = Lists.newArrayList(1L, 20L, 203L);

        PersistenceProcessor persist = mock(PersistenceProcessor.class);
        TimestampOracleImpl timestampOracle = new TimestampOracleImpl(metrics,
                new TimestampOracleImpl.InMemoryTimestampStorage(), new MockPanicker());
        TSOServerConfig config = new TSOServerConfig();
        config.setMaxItems(1000);
        RequestProcessor proc = new RequestProcessorImpl(metrics, timestampOracle,
                                                         persist, new MockPanicker(), config);
        proc.timestampRequest(null);
        ArgumentCaptor<Long> TScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(1)).persistTimestamp(
                TScapture.capture(), any(Channel.class));
        long firstTS = TScapture.getValue();

        proc.commitRequest(firstTS - 1, rows, false, null);
        verify(persist, timeout(100).times(1)).persistAbort(eq(firstTS - 1), anyBoolean(), any(Channel.class));

        proc.commitRequest(firstTS, rows, false, null);
        ArgumentCaptor<Long> commitTScapture = ArgumentCaptor.forClass(Long.class);

        verify(persist, timeout(100).times(1)).persistCommit(eq(firstTS), commitTScapture.capture(),
                                                             any(Channel.class));
        assertTrue("Commit TS must be greater than start TS", commitTScapture.getValue() > firstTS);

        // test conflict
        proc.timestampRequest(null);
        TScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(2)).persistTimestamp(
                TScapture.capture(), any(Channel.class));
        long secondTS = TScapture.getValue();

        proc.timestampRequest(null);
        TScapture = ArgumentCaptor.forClass(Long.class);
        verify(persist, timeout(100).times(3)).persistTimestamp(
                TScapture.capture(), any(Channel.class));
        long thirdTS = TScapture.getValue();

        proc.commitRequest(thirdTS, rows, false, null);
        verify(persist, timeout(100).times(1)).persistCommit(eq(thirdTS), anyLong(),
                                                             any(Channel.class));
        proc.commitRequest(secondTS, rows, false, null);
        verify(persist, timeout(100).times(1)).persistAbort(eq(secondTS), anyBoolean(),
                                                            any(Channel.class));
    }
}

