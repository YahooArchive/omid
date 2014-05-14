package com.yahoo.omid.tso;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.jboss.netty.channel.Channel;

import com.yahoo.omid.committable.CommitTable;
import com.lmax.disruptor.*;
import com.codahale.metrics.MetricRegistry;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Histogram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PersistenceProcessorImpl
    implements EventHandler<PersistenceProcessorImpl.PersistEvent>,
               PersistenceProcessor, TimeoutHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessor.class);

    final ReplyProcessor reply;
    final CommitTable.Client commitTableClient;
    final CommitTable.Writer writer;
    final RingBuffer<PersistEvent> persistRing;

    final int maxBatchSize;

    final Batch batch;

    final Timer flushTimer;
    final Histogram batchSizeHistogram;
    long lastFlush = System.nanoTime();
    final static int BATCH_TIMEOUT_MS = 5;

    PersistenceProcessorImpl(MetricRegistry metrics,
                             CommitTable commitTable, ReplyProcessor reply, int maxBatchSize)
                                     throws InterruptedException, ExecutionException {

        this.commitTableClient = commitTable.getClient().get();
        this.writer = commitTable.getWriter().get();
        this.reply = reply;
        this.maxBatchSize = maxBatchSize;
        
        batch = new Batch(maxBatchSize);

        flushTimer = metrics.timer(name("tso", "persist", "flush"));
        batchSizeHistogram = metrics.histogram(name("tso", "persist", "batchsize"));

        // FIXME consider putting something more like a phased strategy here to avoid
        // all the syscalls
        final TimeoutBlockingWaitStrategy timeoutStrategy
            = new TimeoutBlockingWaitStrategy(BATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        persistRing = RingBuffer.<PersistEvent>createSingleProducer(
                PersistEvent.EVENT_FACTORY, 1<<12, timeoutStrategy);
        SequenceBarrier persistSequenceBarrier = persistRing.newBarrier();
        BatchEventProcessor<PersistEvent> persistProcessor = new BatchEventProcessor<PersistEvent>(
                persistRing,
                persistSequenceBarrier,
                this);
        persistRing.addGatingSequences(persistProcessor.getSequence());


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
            handleAbort(event.getStartTimestamp(), event.isRetry(), event.getChannel());
            break;
        case TIMESTAMP:
            batch.addTimestamp(event.getStartTimestamp(), event.getChannel());
            break;
        }

        if (batch.isFull() || endOfBatch) {
            maybeFlushBatch();
        }
    }

    private void handleAbort(long startTimestamp, boolean isRetry, Channel channel) {

        if(!isRetry) {
            reply.abortResponse(startTimestamp, channel);
            return;
        }

        // 1) Force to flush so the whole batch hits disk
        flush();
        // 2) Check the commit table
        final Optional<Long> commitTimestamp;
        try {
            commitTimestamp = commitTableClient.getCommitTimestamp(startTimestamp).get();
            if(!commitTimestamp.isPresent()) {
                reply.abortResponse(startTimestamp, channel);
            } else {
                reply.commitResponse(startTimestamp, commitTimestamp.get(), channel);
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted reading from commit table");
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.error("Error reading from commit table", e);
        }
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
        if (batch.isFull() || (System.nanoTime() - lastFlush) > TimeUnit.MILLISECONDS.toNanos(BATCH_TIMEOUT_MS)) {
            flush();
        }
    }

    private void flush() {
        lastFlush = System.nanoTime();
        batchSizeHistogram.update(batch.getNumEvents());
        try {
            writer.flush().get();
            flushTimer.update((System.nanoTime() - lastFlush), TimeUnit.NANOSECONDS);
            batch.sendRepliesAndReset(reply);
        } catch (ExecutionException ee) {
            LOG.error("Error persisting commit batch", ee.getCause());
            panic();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted after persistence");
        }
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

    public void panic() {
        // FIXME panic properly, shouldn't call system exit directly
        LOG.error("Ended up in bad state, panicking");
        System.exit(-1);
    }

    public final static class Batch {
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

        void addTimestamp(long startTimestamp, Channel c) {
            if (isFull()) {
                throw new IllegalStateException("batch full");
            }
            int index = numEvents++;
            PersistEvent e = events[index];
            PersistEvent.makePersistTimestamp(e, startTimestamp, c);
        }

        void sendRepliesAndReset(ReplyProcessor reply) {
            for (int i = 0; i < numEvents; i++) {
                PersistEvent e = events[i];
                switch (e.getType()) {
                case TIMESTAMP:
                    reply.timestampResponse(e.getStartTimestamp(), e.getChannel());
                    break;
                case COMMIT:
                    reply.commitResponse(e.getStartTimestamp(), e.getCommitTimestamp(), e.getChannel());
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
            TIMESTAMP, COMMIT, ABORT
        }
        private Type type = null;
        private Channel channel = null;

        private boolean isRetry = false;
        private long startTimestamp = 0;
        private long commitTimestamp = 0;

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

        Type getType() { return type; }
        Channel getChannel() { return channel; }
        boolean isRetry() { return isRetry; }
        long getStartTimestamp() { return startTimestamp; }
        long getCommitTimestamp() { return commitTimestamp; }

        public final static EventFactory<PersistEvent> EVENT_FACTORY
            = new EventFactory<PersistEvent>() {
            public PersistEvent newInstance()
            {
                return new PersistEvent();
            }
        };
    }

}
