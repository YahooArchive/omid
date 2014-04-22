package com.yahoo.omid.tso;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import java.util.ArrayList;
import java.util.List;
import java.util.ArrayList;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;

import com.yahoo.omid.committable.CommitTable;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.*;

import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Gauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PersistenceProcessorImpl
    implements EventHandler<PersistenceProcessorImpl.PersistEvent>,
               PersistenceProcessor, TimeoutHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessor.class);

    final ReplyProcessor reply;
    final CommitTable.Writer writer;
    final RingBuffer<PersistEvent> persistRing;

    // should probably use a ringbuffer for this too
    final static int MAX_BATCH_SIZE = 1000;
    final static int BATCH_COUNT = 1000;

    final BlockingQueue<Batch> batchPool = new ArrayBlockingQueue<Batch>(BATCH_COUNT);
    Batch curBatch = null;

    final Timer flushTimer;
    final Histogram batchSizeHistogram;
    final ExecutorService callbackExec;
    long lastFlush = System.nanoTime();
    final static int BATCH_TIMEOUT_MS = 5;

    PersistenceProcessorImpl(MetricRegistry metrics,
                             CommitTable.Writer writer, ReplyProcessor reply) {
        this.reply = reply;
        this.writer = writer;

        flushTimer = metrics.timer(name("tso", "persist", "flush"));
        batchSizeHistogram = metrics.histogram(name("tso", "persist", "batchsize"));
        metrics.register(name("tso", "persist", "batchPool"), new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return batchPool.size();
                    }
            });

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

        for (int i = 0; i < BATCH_COUNT; i++) {
            batchPool.add(new Batch());
        }

        ExecutorService persistExec = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("persist-%d").build());
        persistExec.submit(persistProcessor);

        callbackExec = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("persist-cb-%d").build());        
    }

    @Override
    public void onEvent(final PersistEvent event, final long sequence, final boolean endOfBatch)
        throws Exception {

        if (curBatch == null) {
            curBatch = batchPool.take();
        }
        
        switch (event.getType()) {
        case COMMIT:
            writer.addCommittedTransaction(event.getStartTimestamp(), event.getCommitTimestamp());
            curBatch.addCommit(event.getStartTimestamp(), event.getCommitTimestamp(), event.getChannel());
            break;
        case TIMESTAMP:
            curBatch.addTimestamp(event.getStartTimestamp(), event.getChannel());
            break;
        }

        if (curBatch.getNumEvents() >= MAX_BATCH_SIZE || endOfBatch) {
            maybeFlushBatch();
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
        if (curBatch == null) {
            return;
        }
        boolean flushNow = false;
        if (curBatch.getNumEvents() >= MAX_BATCH_SIZE) {
            flushNow = true;
        } else if ((System.nanoTime() - lastFlush) > TimeUnit.MILLISECONDS.toNanos(BATCH_TIMEOUT_MS)) {
            flushNow = true;
        }

        if (flushNow) {
            lastFlush = System.nanoTime();
            batchSizeHistogram.update(curBatch.getNumEvents());
            ListenableFuture<Void> f = writer.flush();
            f.addListener(new PersistedListener(f, curBatch, lastFlush), callbackExec);
            curBatch = null;
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
    public void persistAbort(long startTimestamp, Channel c) {
        // nothing is persisted for aborts
        reply.abortResponse(startTimestamp, c);
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

    class PersistedListener implements Runnable {
        ListenableFuture<Void> future;
        Batch batch;
        long startTime;

        PersistedListener(ListenableFuture<Void> future, Batch batch, long startTime) {
            this.future = future;
            this.batch = batch;
            this.startTime = startTime;
        }

        @Override
        public void run() {
            flushTimer.update(startTime, TimeUnit.NANOSECONDS);

            try {
                future.get();
                batch.sendReplies(reply);

                batch.reset();
                batchPool.put(batch);
            } catch (ExecutionException ee) {
                LOG.error("Error persisting commit batch", ee.getCause());
                panic();
                return;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted after persistence");
                return;
            }
        }
    }

    public final static class Batch {
        PersistEvent[] events = new PersistEvent[MAX_BATCH_SIZE];
        int numEvents;

        Batch() {
            numEvents = 0;
            for (int i = 0; i < MAX_BATCH_SIZE; i++) {
                events[i] = new PersistEvent();
            }
        }

        void reset() {
            numEvents = 0;
        }

        int getNumEvents() {
            return numEvents;
        }

        void addCommit(long startTimestamp, long commitTimestamp, Channel c) {
            int index = numEvents++;
            if (numEvents > MAX_BATCH_SIZE) {
                throw new IllegalStateException("batch full");
            }
            PersistEvent e = events[index];
            PersistEvent.makePersistCommit(e, startTimestamp, commitTimestamp, c);
        }

        void addTimestamp(long startTimestamp, Channel c) {
            int index = numEvents++;
            if (numEvents > MAX_BATCH_SIZE) {
                throw new IllegalStateException("batch full");
            }
            PersistEvent e = events[index];
            PersistEvent.makePersistTimestamp(e, startTimestamp, c);
        }

        void sendReplies(ReplyProcessor reply) {
            for (int i = 0; i < numEvents; i++) {
                PersistEvent e = events[i];
                switch (e.getType()) {
                case TIMESTAMP:
                    reply.timestampResponse(e.getStartTimestamp(), e.getChannel());
                    break;
                case COMMIT:
                    reply.commitResponse(e.getStartTimestamp(), e.getCommitTimestamp(), e.getChannel());
                    break;
                }
            }
            numEvents = 0;
        }
    }

    public final static class PersistEvent {
        enum Type {
            TIMESTAMP, COMMIT
        }
        private Type type = null;
        private Channel channel = null;

        private long startTimestamp = 0;
        private long commitTimestamp = 0;

        static void makePersistCommit(PersistEvent e, long startTimestamp,
                                      long commitTimestamp, Channel c) {
            e.type = Type.COMMIT;
            e.startTimestamp = startTimestamp;
            e.commitTimestamp = commitTimestamp;
            e.channel = c;
        }

        static void makePersistTimestamp(PersistEvent e, long startTimestamp, Channel c) {
            e.type = Type.TIMESTAMP;
            e.startTimestamp = startTimestamp;
            e.channel = c;
        }

        Type getType() { return type; }
        Channel getChannel() { return channel; }
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
