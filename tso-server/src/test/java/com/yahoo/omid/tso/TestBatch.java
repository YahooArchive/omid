package com.yahoo.omid.tso;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.jboss.netty.channel.Channel;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBatch {

    private static final Logger LOG = LoggerFactory.getLogger(TestBatch.class);

    private static final int BATCH_SIZE = 1000;

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
        batch.sendRepliesAndReset(replyProcessor, retryProcessor);
        AssertJUnit.assertFalse("Batch shouldn't be full", batch.isFull());
        AssertJUnit.assertEquals("Num events should be 0", 0, batch.getNumEvents());

    }

}
