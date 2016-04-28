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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkerPool;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class PersistenceProcessorImpl implements PersistenceProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessorImpl.class);

    private static final long INITIAL_LWM_VALUE = -1L;

    private final ReplyProcessor replyProcessor;
    private final RingBuffer<PersistBatchEvent> persistRing;

    private final BatchPool batchPool;
    @VisibleForTesting
    Batch batch;

    // TODO Next two need to be either int or AtomicLong
    volatile private long batchIDCnt;
    volatile private long lowWatermark = INITIAL_LWM_VALUE;

    private MonitoringContext lowWatermarkContext;

    @Inject
    PersistenceProcessorImpl(TSOServerConfig config,
                             BatchPool batchPool,
                             ReplyProcessor replyProcessor,
                             Panicker panicker,
                             PersistenceProcessorHandler[] handlers)
            throws InterruptedException, ExecutionException, IOException {

        this.batchIDCnt = 0L;
        this.batchPool = batchPool;
        this.batch = batchPool.getNextEmptyBatch();

        this.replyProcessor = replyProcessor;

        this.persistRing = RingBuffer.createSingleProducer(
                PersistBatchEvent.EVENT_FACTORY, 1 << 20, new BusySpinWaitStrategy());

        WorkerPool<PersistBatchEvent> persistProcessor = new WorkerPool<>(persistRing,
                                                                          persistRing.newBarrier(),
                                                                          new FatalExceptionHandler(panicker),
                                                                          handlers);
        this.persistRing.addGatingSequences(persistProcessor.getWorkerSequences());

        ExecutorService requestExec = Executors.newFixedThreadPool(config.getPersistHandlerNum(),
                                                                   new ThreadFactoryBuilder().setNameFormat("persist-%d").build());
        persistProcessor.start(requestExec);

    }

    @Override
    public void persistFlush() throws InterruptedException {

        if (batch.isEmpty()) {
            return;
        }
        batch.addLowWatermark(this.lowWatermark, this.lowWatermarkContext);
        long seq = persistRing.next();
        PersistBatchEvent e = persistRing.get(seq);
        PersistBatchEvent.makePersistBatch(e, batch, batchIDCnt++);
        persistRing.publish(seq);
        batch = batchPool.getNextEmptyBatch();

    }

    @Override
    public void persistCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx)
            throws InterruptedException {

        batch.addCommit(startTimestamp, commitTimestamp, c, monCtx);
        if (batch.isLastEntryEmpty()) {
            persistFlush();
        }

    }

    @Override
    public void persistAbort(long startTimestamp, boolean isRetry, Channel c, MonitoringContext context)
            throws InterruptedException {

        batch.addAbort(startTimestamp, isRetry, c, context);
        if (batch.isLastEntryEmpty()) {
            persistFlush();
        }

    }

    @Override
    public void persistTimestamp(long startTimestamp, Channel c, MonitoringContext context)
            throws InterruptedException {

        batch.addTimestamp(startTimestamp, c, context);
        if (batch.isLastEntryEmpty()) {
            persistFlush();
        }

    }

    @Override
    public void persistLowWatermark(long lowWatermark, MonitoringContext context) {

        this.lowWatermark = lowWatermark;
        this.lowWatermarkContext = context;

    }

    final static class PersistBatchEvent {

        private Batch batch;
        private long batchID;

        static void makePersistBatch(PersistBatchEvent e, Batch batch, long batchID) {
            e.batch = batch;
            e.batchID = batchID;
        }

        Batch getBatch() {
            return batch;
        }

        long getBatchID() {
            return batchID;
        }

        final static EventFactory<PersistBatchEvent> EVENT_FACTORY = new EventFactory<PersistBatchEvent>() {
            public PersistBatchEvent newInstance() {
                return new PersistBatchEvent();
            }
        };

    }

}
