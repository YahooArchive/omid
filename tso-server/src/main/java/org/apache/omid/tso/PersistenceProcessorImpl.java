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
import com.google.common.base.Objects;
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

import static org.apache.omid.tso.PersistenceProcessorImpl.PersistBatchEvent.EVENT_FACTORY;
import static org.apache.omid.tso.PersistenceProcessorImpl.PersistBatchEvent.makePersistBatch;

class PersistenceProcessorImpl implements PersistenceProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessorImpl.class);

    private static final long INITIAL_LWM_VALUE = -1L;

    private final RingBuffer<PersistBatchEvent> persistRing;

    private final BatchPool batchPool;
    @VisibleForTesting
    Batch currentBatch;

    // TODO Next two need to be either int or AtomicLong
    volatile private long batchSequence;
    volatile private long lowWatermark = INITIAL_LWM_VALUE;

    private MonitoringContext lowWatermarkContext;

    @Inject
    PersistenceProcessorImpl(TSOServerConfig config,
                             BatchPool batchPool,
                             Panicker panicker,
                             PersistenceProcessorHandler[] handlers)
            throws InterruptedException, ExecutionException, IOException {

        this.batchSequence = 0L;
        this.batchPool = batchPool;
        this.currentBatch = batchPool.getNextEmptyBatch();

        // Disruptor configuration
        this.persistRing = RingBuffer.createSingleProducer(EVENT_FACTORY, 1 << 20, new BusySpinWaitStrategy());

        ThreadFactoryBuilder threadFactory = new ThreadFactoryBuilder().setNameFormat("persist-%d");
        ExecutorService requestExec = Executors.newFixedThreadPool(config.getNumConcurrentCTWriters(),
                                                                   threadFactory.build());

        WorkerPool<PersistBatchEvent> persistProcessor = new WorkerPool<>(persistRing,
                                                                          persistRing.newBarrier(),
                                                                          new FatalExceptionHandler(panicker),
                                                                          handlers);
        this.persistRing.addGatingSequences(persistProcessor.getWorkerSequences());
        persistProcessor.start(requestExec);

    }

    @Override
    public void triggerCurrentBatchFlush() throws InterruptedException {

        if (currentBatch.isEmpty()) {
            return;
        }
        currentBatch.addLowWatermark(this.lowWatermark, this.lowWatermarkContext);
        long seq = persistRing.next();
        PersistBatchEvent e = persistRing.get(seq);
        makePersistBatch(e, batchSequence++, currentBatch);
        persistRing.publish(seq);
        currentBatch = batchPool.getNextEmptyBatch();

    }

    @Override
    public void addCommitToBatch(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx)
            throws InterruptedException {

        currentBatch.addCommit(startTimestamp, commitTimestamp, c, monCtx);
        if (currentBatch.isLastEntryEmpty()) {
            triggerCurrentBatchFlush();
        }

    }

    @Override
    public void addAbortToBatch(long startTimestamp, boolean isRetry, Channel c, MonitoringContext context)
            throws InterruptedException {

        currentBatch.addAbort(startTimestamp, isRetry, c, context);
        if (currentBatch.isLastEntryEmpty()) {
            triggerCurrentBatchFlush();
        }

    }

    @Override
    public void addTimestampToBatch(long startTimestamp, Channel c, MonitoringContext context)
            throws InterruptedException {

        currentBatch.addTimestamp(startTimestamp, c, context);
        if (currentBatch.isLastEntryEmpty()) {
            triggerCurrentBatchFlush();
        }

    }

    @Override
    public void addLowWatermarkToBatch(long lowWatermark, MonitoringContext context) {

        this.lowWatermark = lowWatermark;
        this.lowWatermarkContext = context;

    }

    final static class PersistBatchEvent {

        private long batchSequence;
        private Batch batch;

        static void makePersistBatch(PersistBatchEvent e, long batchSequence, Batch batch) {
            e.batch = batch;
            e.batchSequence = batchSequence;
        }

        Batch getBatch() {
            return batch;
        }

        long getBatchSequence() {
            return batchSequence;
        }

        final static EventFactory<PersistBatchEvent> EVENT_FACTORY = new EventFactory<PersistBatchEvent>() {
            public PersistBatchEvent newInstance() {
                return new PersistBatchEvent();
            }
        };

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("batchSequence", batchSequence)
                    .add("batch", batch)
                    .toString();
        }

    }

}
