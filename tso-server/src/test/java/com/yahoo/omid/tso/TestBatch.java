package com.yahoo.omid.tso;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import org.jboss.netty.channel.Channel;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestBatch {

    private static final Logger LOG = LoggerFactory.getLogger(TestBatch.class);

    private static final int BATCH_SIZE = 1000;

    @Mock
    private Channel channel;
    @Mock
    private RetryProcessor retryProcessor;
    @Mock
    private ReplyProcessor replyProcessor;

    // The batch element to test
    private PersistenceProcessorImpl.Batch batch;

    @BeforeMethod(alwaysRun = true)
    public void initMocksAndComponents() {

        MockitoAnnotations.initMocks(this);

        batch = new PersistenceProcessorImpl.Batch(BATCH_SIZE);

    }

    @Test
    public void testBatchFunctionality() {

        // Test initial state is OK
        AssertJUnit.assertFalse("Batch shouldn't be full", batch.isFull());
        AssertJUnit.assertEquals("Num events should be 0", 0, batch.getNumEvents());

        // Test adding a single commit event is OK
        batch.addCommit(0, 1, channel);
        AssertJUnit.assertFalse("Batch shouldn't be full", batch.isFull());
        AssertJUnit.assertEquals("Num events should be 1", 1, batch.getNumEvents());

        // Test when filling the batch with events, batch is full
        for(int i = 0 ; i < (BATCH_SIZE - 1) ; i++) {
            if (i % 2 == 0) {
                batch.addTimestamp(i, channel);
            } else {
                batch.addCommit(i, i + 1, channel);
            }
        }
        AssertJUnit.assertTrue("Batch should be full", batch.isFull());
        AssertJUnit.assertEquals("Num events should be " + BATCH_SIZE, BATCH_SIZE, batch.getNumEvents());

        // Test an exception is thrown when batch is full and a new element is going to be added
        try {
            batch.addCommit(0, 1, channel);
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
               .timestampResponse(anyLong(), any(Channel.class));
        verify(replyProcessor, timeout(100).times(BATCH_SIZE / 2))
                .commitResponse(eq(!SHOULD_MAKE_HEURISTIC_DECISSION), anyLong(), anyLong(),
                any(Channel.class));
        AssertJUnit.assertFalse("Batch shouldn't be full", batch.isFull());
        AssertJUnit.assertEquals("Num events should be 0", 0, batch.getNumEvents());

    }

    @Test
    public void testBatchFunctionalityWhenMastershipIsLost() {

        // Fill the batch with events till full
        for (int i = 0; i < BATCH_SIZE; i++) {
            if (i % 2 == 0) {
                batch.addTimestamp(i, channel);
            } else {
                batch.addCommit(i, i + 1, channel);
            }
        }

        // Test that sending replies empties the batch also when the replica
        // is NOT master and calls the ambiguousCommitResponse() method on the
        // reply processor
        final boolean MASTER_INSTANCE = true;
        final boolean SHOULD_MAKE_HEURISTIC_DECISSION = true;
        batch.sendRepliesAndReset(replyProcessor, retryProcessor, !MASTER_INSTANCE);
        verify(replyProcessor, timeout(100).times(BATCH_SIZE / 2))
            .timestampResponse(anyLong(), any(Channel.class));
        verify(replyProcessor, timeout(100).times(BATCH_SIZE / 2))
            .commitResponse(eq(SHOULD_MAKE_HEURISTIC_DECISSION), anyLong(), anyLong(),any(Channel.class));
        assertFalse(batch.isFull(), "Batch shouldn't be full");
        assertEquals(batch.getNumEvents(), 0, "Num events should be 0");

    }
}
