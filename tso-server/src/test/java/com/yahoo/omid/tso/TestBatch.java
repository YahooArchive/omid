package com.yahoo.omid.tso;

import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import org.jboss.netty.channel.Channel;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestBatch {

    private static final Logger LOG = LoggerFactory.getLogger(TestBatch.class);

    private static final int BATCH_SIZE = 1000;
    private MetricsRegistry metrics = new NullMetricsProvider();

    @Mock
    private Channel channel;
    @Mock
    private RetryProcessor retryProcessor;
    @Mock
    private ReplyProcessor replyProcessor;

    // The batch element to test
    private PersistenceProcessorImpl.Batch batch;

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void initMocksAndComponents() {
        MockitoAnnotations.initMocks(this);
        batch = new PersistenceProcessorImpl.Batch(BATCH_SIZE);
    }

    @Test
    public void testBatchFunctionality() {

        // Required mocks
        Channel channel = Mockito.mock(Channel.class);
        ReplyProcessor replyProcessor = Mockito.mock(ReplyProcessor.class);
        RetryProcessor retryProcessor = Mockito.mock(RetryProcessor.class);

        // The batch element to test
        PersistenceProcessorImpl.Batch batch = new PersistenceProcessorImpl.Batch(BATCH_SIZE);

        // Test initial state is OK
        AssertJUnit.assertFalse("Batch shouldn't be full", batch.isFull());
        AssertJUnit.assertEquals("Num events should be 0", 0, batch.getNumEvents());

        // Test adding a single commit event is OK
        MonitoringContext monCtx = new MonitoringContext(metrics);
        monCtx.timerStart("commitPersistProcessor");
        batch.addCommit(0, 1, channel, monCtx);
        AssertJUnit.assertFalse("Batch shouldn't be full", batch.isFull());
        AssertJUnit.assertEquals("Num events should be 1", 1, batch.getNumEvents());

        // Test when filling the batch with events, batch is full
        for (int i = 0; i < (BATCH_SIZE - 1); i++) {
            if (i % 2 == 0) {
                monCtx = new MonitoringContext(metrics);
                monCtx.timerStart("timestampPersistProcessor");
                batch.addTimestamp(i, channel, monCtx);
            } else {
                monCtx = new MonitoringContext(metrics);
                monCtx.timerStart("commitPersistProcessor");
                batch.addCommit(i, i + 1, channel, monCtx);
            }
        }
        AssertJUnit.assertTrue("Batch should be full", batch.isFull());
        AssertJUnit.assertEquals("Num events should be " + BATCH_SIZE, BATCH_SIZE, batch.getNumEvents());

        // Test an exception is thrown when batch is full and a new element is going to be added
        try {
            monCtx = new MonitoringContext(metrics);
            monCtx.timerStart("commitPersistProcessor");
            batch.addCommit(0, 1, channel, new MonitoringContext(metrics));
            Assert.fail("Should throw an IllegalStateException");
        } catch (IllegalStateException e) {
            AssertJUnit.assertEquals("message returned doesn't match", "batch full", e.getMessage());
            LOG.debug("IllegalStateException catched properly");
        }

        // Test that sending replies empties the batch
        final boolean MASTER_INSTANCE = true;
        final boolean SHOULD_MAKE_HEURISTIC_DECISSION = true;
        batch.sendRepliesAndReset(replyProcessor, retryProcessor, MASTER_INSTANCE);
        verify(replyProcessor, timeout(100).times(BATCH_SIZE / 2))
                .timestampResponse(anyLong(), any(Channel.class), any(MonitoringContext.class));
        verify(replyProcessor, timeout(100).times(BATCH_SIZE / 2))
                .commitResponse(eq(!SHOULD_MAKE_HEURISTIC_DECISSION), anyLong(), anyLong(),
                        any(Channel.class), any(MonitoringContext.class));
        AssertJUnit.assertFalse("Batch shouldn't be full", batch.isFull());
        AssertJUnit.assertEquals("Num events should be 0", 0, batch.getNumEvents());

    }

    @Test
    public void testBatchFunctionalityWhenMastershipIsLost() {
        Channel channel = Mockito.mock(Channel.class);

        // Fill the batch with events till full
        for (int i = 0; i < BATCH_SIZE; i++) {
            if (i % 2 == 0) {
                MonitoringContext monCtx = new MonitoringContext(metrics);
                monCtx.timerStart("timestampPersistProcessor");
                batch.addTimestamp(i, channel, monCtx);
            } else {
                MonitoringContext monCtx = new MonitoringContext(metrics);
                monCtx.timerStart("commitPersistProcessor");
                batch.addCommit(i, i + 1, channel, monCtx);
            }
        }

        // Test that sending replies empties the batch also when the replica
        // is NOT master and calls the ambiguousCommitResponse() method on the
        // reply processor
        final boolean MASTER_INSTANCE = true;
        final boolean SHOULD_MAKE_HEURISTIC_DECISSION = true;
        batch.sendRepliesAndReset(replyProcessor, retryProcessor, !MASTER_INSTANCE);
        verify(replyProcessor, timeout(100).times(BATCH_SIZE / 2))
                .timestampResponse(anyLong(), any(Channel.class), any(MonitoringContext.class));
        verify(replyProcessor, timeout(100).times(BATCH_SIZE / 2))
                .commitResponse(eq(SHOULD_MAKE_HEURISTIC_DECISSION), anyLong(), anyLong(), any(Channel.class), any(
                        MonitoringContext.class));
        assertFalse(batch.isFull(), "Batch shouldn't be full");
        assertEquals(batch.getNumEvents(), 0, "Num events should be 0");

    }
}
