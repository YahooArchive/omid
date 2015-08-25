package com.yahoo.omid.tso;

import static com.codahale.metrics.MetricRegistry.name;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.TimeoutHandler;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.metrics.Histogram;
import com.yahoo.omid.metrics.Meter;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.Timer;

class PersistenceProcessorImpl
    implements EventHandler<PersistenceProcessorImpl.PersistEvent>,
               PersistenceProcessor, TimeoutHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessor.class);

    static final int DEFAULT_MAX_BATCH_SIZE = 10000;
    static final String TSO_MAX_BATCH_SIZE_KEY = "tso.maxbatchsize";
    static final int DEFAULT_BATCH_PERSIST_TIMEOUT_MS = 100;
    static final String TSO_BATCH_PERSIST_TIMEOUT_MS_KEY = "tso.batch-persist-timeout-ms";

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
    long lastFlush = System.nanoTime();

    final static int BATCH_TIMEOUT_MS = 100;

    @Inject
    PersistenceProcessorImpl(MetricsRegistry metrics,
                             LeaseManagement leaseManager,
                             CommitTable commitTable,
                             ReplyProcessor reply,
                             RetryProcessor retryProc,
                             Panicker panicker,
                             TSOServerConfig config)
    throws InterruptedException, ExecutionException {
        this(metrics,
             new Batch(config.getMaxBatchSize()),
             leaseManager,
             commitTable,
             reply,
             retryProc,
             panicker,
             config);
    }

    @VisibleForTesting
    PersistenceProcessorImpl(MetricsRegistry metrics,
                             Batch batch,
                             LeaseManagement leaseManager,
                             CommitTable commitTable,
                             ReplyProcessor reply,
                             RetryProcessor retryProc,
                             Panicker panicker,
                             TSOServerConfig config)
    throws InterruptedException, ExecutionException {

        this.batch = batch;
        this.leaseManager = leaseManager;
        this.commitTableClient = commitTable.getClient().get();
        this.writer = commitTable.getWriter().get();
        this.reply = reply;
        this.retryProc = retryProc;
        this.panicker = panicker;

        LOG.info("Creating the persist processor with batch size {}, and timeout {}ms",
                 config.getMaxBatchSize(), config.getBatchPersistTimeoutMS());

        flushTimer = metrics.timer(name("tso", "persist", "flush"));
        batchSizeHistogram = metrics.histogram(name("tso", "persist", "batchsize"));
        timeoutMeter = metrics.meter(name("tso", "persist", "timeout"));

        // FIXME consider putting something more like a phased strategy here to avoid
        // all the syscalls
        final TimeoutBlockingWaitStrategy timeoutStrategy
            = new TimeoutBlockingWaitStrategy(config.getBatchPersistTimeoutMS(), TimeUnit.MILLISECONDS);

        persistRing = RingBuffer.<PersistEvent>createSingleProducer(
                PersistEvent.EVENT_FACTORY, 1<<20, timeoutStrategy); // 2^20 entries in ringbuffer
        SequenceBarrier persistSequenceBarrier = persistRing.newBarrier();
        BatchEventProcessor<PersistEvent> persistProcessor = new BatchEventProcessor<PersistEvent>(
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
    public void onEvent(final PersistEvent event, final long sequence, final boolean endOfBatch)
        throws Exception {

        switch (event.getType()) {
        case COMMIT:
            // TODO: What happens when the IOException is thrown?
            writer.addCommittedTransaction(event.getStartTimestamp(), event.getCommitTimestamp());
            batch.addCommit(event.getStartTimestamp(), event.getCommitTimestamp(), event.getChannel());
            break;
        case ABORT:
            sendAbortOrIdentifyFalsePositive(event.getStartTimestamp(), event.isRetry(), event.getChannel());
            break;
        case TIMESTAMP:
            batch.addTimestamp(event.getStartTimestamp(), event.getChannel());
            break;
        case LOW_WATERMARK:
            writer.updateLowWatermark(event.getLowWatermark());
            break;
        }

        if (batch.isFull() || endOfBatch) {
            maybeFlushBatch();
        }
    }

    private void sendAbortOrIdentifyFalsePositive(long startTimestamp, boolean isRetry, Channel channel) {

        if(!isRetry) {
            reply.abortResponse(startTimestamp, channel);
            return;
        }

        // If is a retry, we must check if it is a already committed request abort.
        // This can happen because a client could have missed the reply, so it
        // retried the request after a timeout. So we added to the batch and when
        // it's flushed we'll add events to the retry processor in order to check
        // for false positive aborts. It needs to be done after the flush in case
        // the commit has occurred but it hasn't been persisted yet.
        batch.addUndecidedRetriedRequest(startTimestamp, channel);
    }

    // no event has been received in the timeout period
    @Override
    public void onTimeout(final long sequence) {
        maybeFlushBatch();
    }

    /**
     * Flush the current batch if it is larger than the max batch size,
     * or BATCH_TIMEOUT_MS milliseconds have elapsed since the last flush.
     */
    private void maybeFlushBatch() {
        if (batch.isFull()) {
            flush();
        } else if ((System.nanoTime() - lastFlush) > TimeUnit.MILLISECONDS.toNanos(BATCH_TIMEOUT_MS)) {
            timeoutMeter.mark();
            flush();
        }
    }

    @VisibleForTesting
    void flush() {
        lastFlush = System.nanoTime();

        boolean areWeStillMaster = true;
        if (!leaseManager.stillInLeasePeriod()) {
            // The master TSO replica has changed, so we must inform the
            // clients about it when sending the replies and avoid flushing
            // the current batch of TXs
            areWeStillMaster = false;
            // We need also to clear the data in the buffer
            writer.clearWriteBuffer();
            LOG.trace("This replica lost mastership before flushig data");
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
                LOG.warn("This replica lost mastership after flushing data");
            }
        }
        flushTimer.update((System.nanoTime() - lastFlush));
        batch.sendRepliesAndReset(reply, retryProc, areWeStillMaster);

    }

    @Override
    public void persistCommit(long startTimestamp, long commitTimestamp, Channel c) {
        long seq = persistRing.next();
        PersistEvent e = persistRing.get(seq);
        PersistEvent.makePersistCommit(e, startTimestamp, commitTimestamp, c);
        persistRing.publish(seq);
    }

    @Override
    public void persistAbort(long startTimestamp, boolean isRetry, Channel c) {
        long seq = persistRing.next();
        PersistEvent e = persistRing.get(seq);
        PersistEvent.makePersistAbort(e, startTimestamp, isRetry, c);
        persistRing.publish(seq);
    }

    @Override
    public void persistTimestamp(long startTimestamp, Channel c) {
        long seq = persistRing.next();
        PersistEvent e = persistRing.get(seq);
        PersistEvent.makePersistTimestamp(e, startTimestamp, c);
        persistRing.publish(seq);
    }

    @Override
    public void persistLowWatermark(long lowWatermark) {
        long seq = persistRing.next();
        PersistEvent e = persistRing.get(seq);
        PersistEvent.makePersistLowWatermark(e, lowWatermark);
        persistRing.publish(seq);
    }

    public static class Batch {
        final PersistEvent[] events;
        final int maxBatchSize;
        int numEvents;

        Batch(int maxBatchSize) {
            assert(maxBatchSize > 0);
            this.maxBatchSize = maxBatchSize;
            events = new PersistEvent[maxBatchSize];
            numEvents = 0;
            for (int i = 0; i < maxBatchSize; i++) {
                events[i] = new PersistEvent();
            }
        }

        boolean isFull() {
            assert(numEvents <= maxBatchSize);
            return numEvents == maxBatchSize;
        }

        int getNumEvents() {
            return numEvents;
        }

        void addCommit(long startTimestamp, long commitTimestamp, Channel c) {
            if (isFull()) {
                throw new IllegalStateException("batch full");
            }
            int index = numEvents++;
            PersistEvent e = events[index];
            PersistEvent.makePersistCommit(e, startTimestamp, commitTimestamp, c);
        }

        void addUndecidedRetriedRequest(long startTimestamp, Channel c) {
            if (isFull()) {
                throw new IllegalStateException("batch full");
            }
            int index = numEvents++;
            PersistEvent e = events[index];
            // We mark the event as an ABORT retry to identify the events to send
            // to the retry processor
            PersistEvent.makePersistAbort(e, startTimestamp, true, c);
        }

        void addTimestamp(long startTimestamp, Channel c) {
            if (isFull()) {
                throw new IllegalStateException("batch full");
            }
            int index = numEvents++;
            PersistEvent e = events[index];
            PersistEvent.makePersistTimestamp(e, startTimestamp, c);
        }

        void sendRepliesAndReset(ReplyProcessor reply, RetryProcessor retryProc, boolean isTSOInstanceMaster) {
            for (int i = 0; i < numEvents; i++) {
                PersistEvent e = events[i];
                switch (e.getType()) {
                case TIMESTAMP:
                    reply.timestampResponse(e.getStartTimestamp(), e.getChannel());
                    break;
                case COMMIT:
                    if (isTSOInstanceMaster) {
                        reply.commitResponse(false, e.getStartTimestamp(), e.getCommitTimestamp(), e.getChannel());
                    } else {
                        // The client will need to perform heuristic actions to determine the output
                        reply.commitResponse(true, e.getStartTimestamp(), e.getCommitTimestamp(), e.getChannel());
                    }
                    break;
                case ABORT:
                    if(e.isRetry()) {
                        retryProc.disambiguateRetryRequestHeuristically(e.getStartTimestamp(), e.getChannel());
                    } else {
                        LOG.error("We should not be receiving non-retried aborted requests in here");
                        assert(false);
                    }
                    break;
                default:
                    assert(false);
                    break;
                }
            }
            numEvents = 0;
        }

    }

    public final static class PersistEvent {
        enum Type {
            TIMESTAMP, COMMIT, ABORT, LOW_WATERMARK
        }
        private Type type = null;
        private Channel channel = null;

        private boolean isRetry = false;
        private long startTimestamp = 0;
        private long commitTimestamp = 0;
        private long lowWatermark;

        static void makePersistCommit(PersistEvent e, long startTimestamp,
                                      long commitTimestamp, Channel c) {
            e.type = Type.COMMIT;
            e.startTimestamp = startTimestamp;
            e.commitTimestamp = commitTimestamp;
            e.channel = c;
        }

        static void makePersistAbort(PersistEvent e, long startTimestamp,
                boolean isRetry, Channel c) {
            e.type = Type.ABORT;
            e.startTimestamp = startTimestamp;
            e.isRetry = isRetry;
            e.channel = c;
        }

        static void makePersistTimestamp(PersistEvent e, long startTimestamp, Channel c) {
            e.type = Type.TIMESTAMP;
            e.startTimestamp = startTimestamp;
            e.channel = c;
        }

        static void makePersistLowWatermark(PersistEvent e, long lowWatermark) {
            e.type = Type.LOW_WATERMARK;
            e.lowWatermark = lowWatermark;
        }

        Type getType() { return type; }
        Channel getChannel() { return channel; }
        boolean isRetry() { return isRetry; }
        long getStartTimestamp() { return startTimestamp; }
        long getCommitTimestamp() { return commitTimestamp; }
        long getLowWatermark() { return lowWatermark; }

        public final static EventFactory<PersistEvent> EVENT_FACTORY
            = new EventFactory<PersistEvent>() {
            @Override
            public PersistEvent newInstance()
            {
                return new PersistEvent();
            }
        };
    }

}
