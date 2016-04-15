/**
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
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.TimeoutHandler;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.Histogram;
import org.apache.omid.metrics.Meter;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.Timer;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.omid.metrics.MetricsUtils.name;
import static org.apache.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

class PersistenceProcessorImpl
        implements EventHandler<PersistenceProcessorImpl.PersistEvent>, PersistenceProcessor, TimeoutHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessor.class);

    private final String tsoHostAndPort;
    private final LeaseManagement leaseManager;
    final ReplyProcessor reply;
    final RetryProcessor retryProc;
    final CommitTable.Client commitTableClient;
    final CommitTable.Writer writer;
    final Panicker panicker;
    final RingBuffer<PersistEvent> persistRing;
    final Batch batch;
    final Timer flushTimer;
    final Histogram batchSizeHistogram;
    final Meter timeoutMeter;
    final int batchPersistTimeoutInMs;

    long lastFlush = System.nanoTime();

    @Inject
    PersistenceProcessorImpl(TSOServerConfig config,
                             MetricsRegistry metrics,
                             @Named(TSO_HOST_AND_PORT_KEY) String tsoHostAndPort,
                             LeaseManagement leaseManager,
                             CommitTable commitTable,
                             ReplyProcessor reply,
                             RetryProcessor retryProc,
                             Panicker panicker)
            throws IOException {

        this(config,
             metrics,
             tsoHostAndPort,
             new Batch(config.getMaxBatchSize()),
             leaseManager,
             commitTable,
             reply,
             retryProc,
             panicker);

    }

    @VisibleForTesting
    PersistenceProcessorImpl(TSOServerConfig config,
                             MetricsRegistry metrics,
                             String tsoHostAndPort,
                             Batch batch,
                             LeaseManagement leaseManager,
                             CommitTable commitTable,
                             ReplyProcessor reply,
                             RetryProcessor retryProc,
                             Panicker panicker)
            throws IOException {

        this.tsoHostAndPort = tsoHostAndPort;
        this.batch = batch;
        this.batchPersistTimeoutInMs = config.getBatchPersistTimeoutInMs();
        this.leaseManager = leaseManager;
        this.commitTableClient = commitTable.getClient();
        this.writer = commitTable.getWriter();
        this.reply = reply;
        this.retryProc = retryProc;
        this.panicker = panicker;

        LOG.info("Creating the persist processor with batch size {}, and timeout {}ms",
                 config.getMaxBatchSize(), batchPersistTimeoutInMs);

        flushTimer = metrics.timer(name("tso", "persist", "flush"));
        batchSizeHistogram = metrics.histogram(name("tso", "persist", "batchsize"));
        timeoutMeter = metrics.meter(name("tso", "persist", "timeout"));

        // FIXME consider putting something more like a phased strategy here to avoid
        // all the syscalls
        final TimeoutBlockingWaitStrategy timeoutStrategy
                = new TimeoutBlockingWaitStrategy(config.getBatchPersistTimeoutInMs(), TimeUnit.MILLISECONDS);

        persistRing = RingBuffer.createSingleProducer(
                PersistEvent.EVENT_FACTORY, 1 << 20, timeoutStrategy); // 2^20 entries in ringbuffer
        SequenceBarrier persistSequenceBarrier = persistRing.newBarrier();
        BatchEventProcessor<PersistEvent> persistProcessor = new BatchEventProcessor<>(
                persistRing,
                persistSequenceBarrier,
                this);
        persistRing.addGatingSequences(persistProcessor.getSequence());
        persistProcessor.setExceptionHandler(new FatalExceptionHandler(panicker));

        ExecutorService persistExec = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("persist-%d").build());
        persistExec.submit(persistProcessor);

    }

    @Override
    public void onEvent(PersistEvent event, long sequence, boolean endOfBatch) throws Exception {

        switch (event.getType()) {
            case COMMIT:
                event.getMonCtx().timerStart("commitPersistProcessor");
                // TODO: What happens when the IOException is thrown?
                writer.addCommittedTransaction(event.getStartTimestamp(), event.getCommitTimestamp());
                batch.addCommit(event.getStartTimestamp(), event.getCommitTimestamp(), event.getChannel(),
                                event.getMonCtx());
                break;
            case ABORT:
                sendAbortOrIdentifyFalsePositive(event.getStartTimestamp(), event.isRetry(), event.getChannel(),
                                                 event.getMonCtx());
                break;
            case TIMESTAMP:
                event.getMonCtx().timerStart("timestampPersistProcessor");
                batch.addTimestamp(event.getStartTimestamp(), event.getChannel(), event.getMonCtx());
                break;
        }
        if (batch.isFull() || endOfBatch) {
            maybeFlushBatch();
        }

    }

    private void sendAbortOrIdentifyFalsePositive(long startTimestamp, boolean isRetry, Channel channel,
                                                  MonitoringContext monCtx) {

        if (!isRetry) {
            reply.abortResponse(startTimestamp, channel, monCtx);
            return;
        }

        // If is a retry, we must check if it is a already committed request abort.
        // This can happen because a client could have missed the reply, so it
        // retried the request after a timeout. So we added to the batch and when
        // it's flushed we'll add events to the retry processor in order to check
        // for false positive aborts. It needs to be done after the flush in case
        // the commit has occurred but it hasn't been persisted yet.
        batch.addUndecidedRetriedRequest(startTimestamp, channel, monCtx);
    }

    // no event has been received in the timeout period
    @Override
    public void onTimeout(final long sequence) {
        maybeFlushBatch();
    }

    /**
     * Flush the current batch if it's full, or the timeout has been elapsed since the last flush.
     */
    private void maybeFlushBatch() {
        if (batch.isFull()) {
            flush();
        } else if ((System.nanoTime() - lastFlush) > TimeUnit.MILLISECONDS.toNanos(batchPersistTimeoutInMs)) {
            timeoutMeter.mark();
            flush();
        }
    }

    @VisibleForTesting
    synchronized void flush() {
        lastFlush = System.nanoTime();

        boolean areWeStillMaster = true;
        if (!leaseManager.stillInLeasePeriod()) {
            // The master TSO replica has changed, so we must inform the
            // clients about it when sending the replies and avoid flushing
            // the current batch of TXs
            areWeStillMaster = false;
            // We need also to clear the data in the buffer
            writer.clearWriteBuffer();
            LOG.trace("Replica {} lost mastership before flushing data", tsoHostAndPort);
        } else {
            try {
                writer.flush();
            } catch (IOException e) {
                panicker.panic("Error persisting commit batch", e.getCause());
            }
            batchSizeHistogram.update(batch.getNumEvents());
            if (!leaseManager.stillInLeasePeriod()) {
                // If after flushing this TSO server is not the master
                // replica we need inform the client about it
                areWeStillMaster = false;
                LOG.warn("Replica {} lost mastership after flushing data", tsoHostAndPort);
            }
        }
        flushTimer.update((System.nanoTime() - lastFlush));
        batch.sendRepliesAndReset(reply, retryProc, areWeStillMaster);

    }

    @Override
    public void persistCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx) {
        long seq = persistRing.next();
        PersistEvent e = persistRing.get(seq);
        PersistEvent.makePersistCommit(e, startTimestamp, commitTimestamp, c, monCtx);
        persistRing.publish(seq);
    }

    @Override
    public void persistAbort(long startTimestamp, boolean isRetry, Channel c, MonitoringContext monCtx) {
        long seq = persistRing.next();
        PersistEvent e = persistRing.get(seq);
        PersistEvent.makePersistAbort(e, startTimestamp, isRetry, c, monCtx);
        persistRing.publish(seq);
    }

    @Override
    public void persistTimestamp(long startTimestamp, Channel c, MonitoringContext monCtx) {
        long seq = persistRing.next();
        PersistEvent e = persistRing.get(seq);
        PersistEvent.makePersistTimestamp(e, startTimestamp, c, monCtx);
        persistRing.publish(seq);
    }

    @Override
    public void persistLowWatermark(long lowWatermark) {
        try {
            writer.updateLowWatermark(lowWatermark);
        } catch (IOException e) {
            LOG.error("Should not be thrown");
        }
    }

    public static class Batch {

        final PersistEvent[] events;
        final int maxBatchSize;
        int numEvents;

        Batch(int maxBatchSize) {
            assert (maxBatchSize > 0);
            this.maxBatchSize = maxBatchSize;
            events = new PersistEvent[maxBatchSize];
            numEvents = 0;
            for (int i = 0; i < maxBatchSize; i++) {
                events[i] = new PersistEvent();
            }
        }

        boolean isFull() {
            assert (numEvents <= maxBatchSize);
            return numEvents == maxBatchSize;
        }

        int getNumEvents() {
            return numEvents;
        }

        void addCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx) {
            if (isFull()) {
                throw new IllegalStateException("batch full");
            }
            int index = numEvents++;
            PersistEvent e = events[index];
            PersistEvent.makePersistCommit(e, startTimestamp, commitTimestamp, c, monCtx);
        }

        void addUndecidedRetriedRequest(long startTimestamp, Channel c, MonitoringContext monCtx) {
            if (isFull()) {
                throw new IllegalStateException("batch full");
            }
            int index = numEvents++;
            PersistEvent e = events[index];
            // We mark the event as an ABORT retry to identify the events to send
            // to the retry processor
            PersistEvent.makePersistAbort(e, startTimestamp, true, c, monCtx);
        }

        void addTimestamp(long startTimestamp, Channel c, MonitoringContext monCtx) {
            if (isFull()) {
                throw new IllegalStateException("batch full");
            }
            int index = numEvents++;
            PersistEvent e = events[index];
            PersistEvent.makePersistTimestamp(e, startTimestamp, c, monCtx);
        }

        void sendRepliesAndReset(ReplyProcessor reply, RetryProcessor retryProc, boolean isTSOInstanceMaster) {
            for (int i = 0; i < numEvents; i++) {
                PersistEvent e = events[i];
                switch (e.getType()) {
                    case TIMESTAMP:
                        e.getMonCtx().timerStop("timestampPersistProcessor");
                        reply.timestampResponse(e.getStartTimestamp(), e.getChannel(), e.getMonCtx());
                        break;
                    case COMMIT:
                        e.getMonCtx().timerStop("commitPersistProcessor");
                        if (isTSOInstanceMaster) {
                            reply.commitResponse(false, e.getStartTimestamp(), e.getCommitTimestamp(), e.getChannel(),
                                                 e.getMonCtx());
                        } else {
                            // The client will need to perform heuristic actions to determine the output
                            reply.commitResponse(true, e.getStartTimestamp(), e.getCommitTimestamp(), e.getChannel(),
                                                 e.getMonCtx());
                        }
                        break;
                    case ABORT:
                        if (e.isRetry()) {
                            retryProc.disambiguateRetryRequestHeuristically(e.getStartTimestamp(), e.getChannel(),
                                                                            e.getMonCtx());
                        } else {
                            LOG.error("We should not be receiving non-retried aborted requests in here");
                        }
                        break;
                    default:
                        LOG.error("We should receive only COMMIT or ABORT event types. Received {}", e.getType());
                        break;
                }
            }
            numEvents = 0;
        }

    }

    public final static class PersistEvent {

        private MonitoringContext monCtx;

        enum Type {
            TIMESTAMP, COMMIT, ABORT
        }

        private Type type = null;
        private Channel channel = null;

        private boolean isRetry = false;
        private long startTimestamp = 0;
        private long commitTimestamp = 0;

        static void makePersistCommit(PersistEvent e, long startTimestamp, long commitTimestamp, Channel c,
                                      MonitoringContext monCtx) {
            e.type = Type.COMMIT;
            e.startTimestamp = startTimestamp;
            e.commitTimestamp = commitTimestamp;
            e.channel = c;
            e.monCtx = monCtx;
        }

        static void makePersistAbort(PersistEvent e, long startTimestamp, boolean isRetry, Channel c,
                                     MonitoringContext monCtx) {
            e.type = Type.ABORT;
            e.startTimestamp = startTimestamp;
            e.isRetry = isRetry;
            e.channel = c;
            e.monCtx = monCtx;
        }

        static void makePersistTimestamp(PersistEvent e, long startTimestamp, Channel c, MonitoringContext monCtx) {
            e.type = Type.TIMESTAMP;
            e.startTimestamp = startTimestamp;
            e.channel = c;
            e.monCtx = monCtx;
        }

        MonitoringContext getMonCtx() {
            return monCtx;
        }

        Type getType() {
            return type;
        }

        Channel getChannel() {
            return channel;
        }

        boolean isRetry() {
            return isRetry;
        }

        long getStartTimestamp() {
            return startTimestamp;
        }

        long getCommitTimestamp() {
            return commitTimestamp;
        }

        public final static EventFactory<PersistEvent> EVENT_FACTORY = new EventFactory<PersistEvent>() {
            @Override
            public PersistEvent newInstance() {
                return new PersistEvent();
            }
        };
    }

}
