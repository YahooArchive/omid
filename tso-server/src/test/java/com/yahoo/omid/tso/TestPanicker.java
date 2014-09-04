package com.yahoo.omid.tso;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import com.yahoo.omid.timestamp.storage.TimestampStorage;

public class TestPanicker {

    private static final Logger LOG = LoggerFactory.getLogger(TestPanicker.class);

    MetricsRegistry metrics = new NullMetricsProvider();

    @Test
    public void testTimestampOraclePanic() throws Exception {
        TimestampStorage storage = spy(new TimestampOracleImpl.InMemoryTimestampStorage());
        Panicker panicker = spy(new MockPanicker());

        doThrow(new RuntimeException("Out of memory or something"))
            .when(storage).updateMaxTimestamp(anyLong(), anyLong());

        final TimestampOracleImpl tso = new TimestampOracleImpl(metrics,
                storage, panicker);
        Thread allocThread = new Thread("AllocThread") {
                @Override
                public void run() {
                    try {
                        while (true) {
                            tso.next();
                        }
                    } catch (IOException ioe) {
                        LOG.error("Shouldn't occur");
                    }
                }
            };
        allocThread.start();

        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));
    }

    @Test
    public void testCommitTablePanic() throws Exception {
        Panicker panicker = spy(new MockPanicker());

        SettableFuture<Void> f = SettableFuture.<Void>create();
        f.setException(new IOException("Unable to write"));
        final CommitTable.Writer mockWriter = mock(CommitTable.Writer.class);
        doReturn(f).when(mockWriter).flush();

        final CommitTable.Client mockClient = mock(CommitTable.Client.class);
        CommitTable commitTable = new CommitTable() {
                @Override
                public ListenableFuture<Writer> getWriter() {
                    SettableFuture<Writer> f = SettableFuture.<Writer>create();
                    f.set(mockWriter);
                    return f;
                }

                @Override
                public ListenableFuture<Client> getClient() {
                    SettableFuture<Client> f = SettableFuture.<Client>create();
                    f.set(mockClient);
                    return f;
                }
            };
        PersistenceProcessor proc = new PersistenceProcessorImpl(metrics,
                                                                 commitTable,
                                                                 mock(ReplyProcessor.class),
                                                                 mock(RetryProcessor.class),
                                                                 panicker,
                                                                 new TSOServerConfig());
        proc.persistCommit(1, 2, null);
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));
    }

    @Test
    public void testRuntimeExceptionTakesDownDaemon() throws Exception {
        Panicker panicker = spy(new MockPanicker());

        final CommitTable.Writer mockWriter = mock(CommitTable.Writer.class);
        doThrow(new RuntimeException("Kaboom!"))
            .when(mockWriter).addCommittedTransaction(anyLong(),anyLong());

        final CommitTable.Client mockClient = mock(CommitTable.Client.class);
        CommitTable commitTable = new CommitTable() {
                @Override
                public ListenableFuture<Writer> getWriter() {
                    SettableFuture<Writer> f = SettableFuture.<Writer>create();
                    f.set(mockWriter);
                    return f;
                }

                @Override
                public ListenableFuture<Client> getClient() {
                    SettableFuture<Client> f = SettableFuture.<Client>create();
                    f.set(mockClient);
                    return f;
                }
            };
        PersistenceProcessor proc = new PersistenceProcessorImpl(metrics,
                                                                 commitTable,
                                                                 mock(ReplyProcessor.class),
                                                                 mock(RetryProcessor.class),
                                                                 panicker,
                                                                 new TSOServerConfig());
        proc.persistCommit(1, 2, null);
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));
    }
}
