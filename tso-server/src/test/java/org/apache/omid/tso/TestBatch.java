/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.tso;

import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.jboss.netty.channel.Channel;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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
    private Batch batch;

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void initMocksAndComponents() {
        MockitoAnnotations.initMocks(this);
        batch = new Batch(BATCH_SIZE);
    }

    // TODO. Check this test with the contents of the master branch. See commented asserts below
    @Test
    public void testBatchFunctionality() {

        // Required mocks
        Channel channel = Mockito.mock(Channel.class);
        ReplyProcessor replyProcessor = Mockito.mock(ReplyProcessor.class);
        RetryProcessor retryProcessor = Mockito.mock(RetryProcessor.class);

        // The batch element to test
        Batch batch = new Batch(BATCH_SIZE);

        // Test initial state is OK
        assertFalse(batch.isFull(), "Batch shouldn't be full");
        assertEquals(batch.getNumEvents(), 0, "Num events should be 0");

        // Test adding a single commit event is OK
        MonitoringContext monCtx = new MonitoringContext(metrics);
        monCtx.timerStart("commitPersistProcessor");
        batch.addCommit(0, 1, channel, monCtx);
        assertFalse(batch.isFull(), "Batch shouldn't be full");
        assertEquals(batch.getNumEvents(), 1, "Num events should be 1");

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
        assertTrue(batch.isFull(), "Batch should be full");
        assertEquals(batch.getNumEvents(), BATCH_SIZE, "Num events should be " + BATCH_SIZE);

        // Test an exception is thrown when batch is full and a new element is going to be added
        try {
            monCtx = new MonitoringContext(metrics);
            monCtx.timerStart("commitPersistProcessor");
            batch.addCommit(0, 1, channel, new MonitoringContext(metrics));
            fail("Should throw an IllegalStateException");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "batch is full", "message returned doesn't match");
            LOG.debug("IllegalStateException catched properly");
        }

        // Test that sending replies empties the batch
// TODO. Fix these asserts in new code
//<<<<<<< HEAD
//        batch.sendRepliesAndReset(replyProcessor, retryProcessor);
//        verify(replyProcessor, timeout(100).times(BATCH_SIZE / 2))
//                .timestampResponse(anyLong(), any(Channel.class), any(MonitoringContext.class));
//        verify(replyProcessor, timeout(100).times(BATCH_SIZE / 2))
//                .commitResponse(anyLong(), anyLong(), any(Channel.class), any(MonitoringContext.class));
//        assertFalse(batch.isFull(), "Batch shouldn't be full");
//        assertEquals(batch.getNumEvents(), 0, "Num events should be 0");
//=======
        batch.sendReply(replyProcessor, retryProcessor, (-1));
        verify(replyProcessor, timeout(100).times(1)).batchResponse(batch, (-1));
        assertTrue(batch.isFull(), "Batch shouldn't be empty");
    }

    // TODO Check this test with the contents of the master branch
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
        batch.sendReply(replyProcessor, retryProcessor, (-1));
        verify(replyProcessor, timeout(100).times(1))
               .batchResponse(batch, (-1));
        assertTrue(batch.isFull(), "Batch should be full");
    }

}
