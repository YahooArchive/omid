package com.yahoo.omid.tso;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import com.yahoo.omid.tso.PersistenceProcessorImpl.Batch;

public class TestPersistenceProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TestPersistenceProcessor.class);

    @Mock
    private Batch batch;
    @Mock
    private CommitTable.Writer mockWriter;
    @Mock
    private CommitTable.Client mockClient;
    @Mock
    private RetryProcessor retryProcessor;
    @Mock
    private ReplyProcessor replyProcessor;
    @Mock
    private Panicker panicker;

    private MetricsRegistry metrics;
    private CommitTable commitTable;

    @BeforeMethod(alwaysRun = true)
    public void initMocksAndComponents() {

        MockitoAnnotations.initMocks(this);

        // Configure mock writer to flush successfully
        SettableFuture<Void> f = SettableFuture.<Void> create();
        f.set(null);
        doReturn(f).when(mockWriter).flush();

        // Configure null metrics provider
        metrics = new NullMetricsProvider();

        // Configure commit table to return the mocked writer and client
        commitTable = new CommitTable() {
            @Override
            public ListenableFuture<Writer> getWriter() {
                SettableFuture<Writer> f = SettableFuture.<Writer> create();
                f.set(mockWriter);
                return f;
            }

            @Override
            public ListenableFuture<Client> getClient() {
                SettableFuture<Client> f = SettableFuture.<Client> create();
                f.set(mockClient);
                return f;
            }
        };
    }

    @Test
    public void testCommitPersistenceWithNonHALeaseManager() throws Exception {

        // Init a non-HA lease manager
        NonHALeaseManager leaseManager = spy(new NonHALeaseManager(mock(TSOStateManager.class)));
        // Component under test
        PersistenceProcessor proc = new PersistenceProcessorImpl(metrics,
                                                                 batch,
                                                                 leaseManager,
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker,
                                                                 new TSOServerConfig());

        // The non-ha lease manager always return true for
        // stillInLeasePeriod(), so verify the batch sends replies as master
        proc.persistCommit(1, 2, null);
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batch, timeout(1000).times(2)).sendRepliesAndReset(any(ReplyProcessor.class),
                                                                  any(RetryProcessor.class),
                                                                  eq(true));
    }

    @Test
    public void testCommitPersistenceWithHALeaseManager() throws Exception {

        // Init a HA lease manager
        LeaseManager leaseManager = mock(LeaseManager.class);
        // Component under test
        PersistenceProcessor proc = new PersistenceProcessorImpl(metrics,
                                                                 batch,
                                                                 leaseManager,
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker,
                                                                 new TSOServerConfig());

        // Configure the lease manager to always return true for
        // stillInLeasePeriod, so verify the batch sends replies as master
        doReturn(true).when(leaseManager).stillInLeasePeriod();
        proc.persistCommit(1, 2, null);
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batch).sendRepliesAndReset(any(ReplyProcessor.class), any(RetryProcessor.class), eq(true));

        // Configure the lease manager to always return true first and false
        // later for stillInLeasePeriod, so verify the batch sends replies as
        // non-master
        reset(leaseManager);
        reset(batch);
        doReturn(true).doReturn(false).when(leaseManager).stillInLeasePeriod();
        proc.persistCommit(1, 2, null);
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batch).sendRepliesAndReset(any(ReplyProcessor.class), any(RetryProcessor.class), eq(false));

        // Configure the lease manager to always return false for
        // stillInLeasePeriod, so verify the batch sends replies as non-master
        reset(leaseManager);
        reset(batch);
        doReturn(false).when(leaseManager).stillInLeasePeriod();
        proc.persistCommit(1, 2, null);
        verify(leaseManager, timeout(1000).times(1)).stillInLeasePeriod();
        verify(batch).sendRepliesAndReset(any(ReplyProcessor.class), any(RetryProcessor.class), eq(false));
    }

    @Test
    public void testCommitTableExceptionOnCommitPersistenceTakesDownDaemon() throws Exception {


        // Init lease management (doesn't matter if HA or not)
        LeaseManagement leaseManager = mock(LeaseManagement.class);
        PersistenceProcessor proc = new PersistenceProcessorImpl(metrics,
                                                                 leaseManager,
                                                                 commitTable,
                                                                 mock(ReplyProcessor.class),
                                                                 mock(RetryProcessor.class),
                                                                 panicker,
                                                                 new TSOServerConfig());

        // Configure lease manager to work normally
        doReturn(true).when(leaseManager).stillInLeasePeriod();

        // Configure commit table writer to explode when flushing changes to DB
        SettableFuture<Void> f = SettableFuture.<Void> create();
        f.setException(new IOException("Unable to write"));
        doReturn(f).when(mockWriter).flush();

        // Check the panic is extended!
        proc.persistCommit(1, 2, null);
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));
    }

    @Test
    public void testRuntimeExceptionOnCommitPersistenceTakesDownDaemon() throws Exception {

        PersistenceProcessor proc = new PersistenceProcessorImpl(metrics,
                                                                 mock(LeaseManagement.class),
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker,
                                                                 new TSOServerConfig());

        // Configure writer to explode with a runtime exception
        doThrow(new RuntimeException("Kaboom!")).when(mockWriter).addCommittedTransaction(anyLong(), anyLong());

        // Check the panic is extended!
        proc.persistCommit(1, 2, null);
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));
    }

}
