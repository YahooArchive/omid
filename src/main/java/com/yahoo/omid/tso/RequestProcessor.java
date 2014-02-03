package com.yahoo.omid.tso;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.List;

import org.jboss.netty.channel.ChannelHandlerContext;

import com.yahoo.omid.tso.messages.AbortRequest;
import com.yahoo.omid.tso.messages.CommitQueryRequest;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.FullAbortRequest;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.metrics.StatusOracleMetrics;
import com.yahoo.omid.tso.persistence.LoggerProtocol;
import com.yammer.metrics.core.TimerContext;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

import com.yahoo.omid.tso.SharedBufferProcessor.SharedBufEvent;
import com.yahoo.omid.tso.ReplyProcessor.ReplyEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RequestProcessor implements EventHandler<TSOEvent>
{
    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessor.class);

    TSOState sharedState;

    StatusOracleMetrics metrics = new StatusOracleMetrics();
    private TimestampOracle timestampOracle = null;

    final RingBuffer<ReplyEvent> responseRing;
    final RingBuffer<SharedBufEvent> sharedBufRing;
    final WALProcessor wal;

    RequestProcessor(TSOState sharedState, RingBuffer<ReplyEvent> responseRing,
                     RingBuffer<SharedBufEvent> sharedBufRing, WALProcessor walProcessor) {
        timestampOracle = sharedState.getSO();
        this.sharedState = sharedState;
        this.responseRing = responseRing;
        this.sharedBufRing = sharedBufRing;
        this.wal = walProcessor;
    }

    @Override
    public void onEvent(final TSOEvent event, final long sequence, final boolean endOfBatch)
        throws Exception
    {
        // process a new event as it becomes available.
        Object msg = event.getMessage();
        ChannelHandlerContext ctx = event.getContext();

        if (msg instanceof TimestampRequest) {
            handleTimestamp((TimestampRequest) msg, ctx);
        } else if (msg instanceof CommitRequest) {
            handleCommit((CommitRequest) msg, ctx);
        } else if (msg instanceof AbortRequest) {
            handleAbort((AbortRequest) msg, ctx);
        } else if (msg instanceof FullAbortRequest) {
            handleFullAbort((FullAbortRequest) msg, ctx);
        } else if (msg instanceof CommitQueryRequest) {
            handleCommitQuery((CommitQueryRequest) msg, ctx);
        }
    }

    public void handleAbort(AbortRequest msg, ChannelHandlerContext ctx) {
        if (msg.startTimestamp < sharedState.largestDeletedTimestamp) {
            LOG.warn("Too old starttimestamp, already aborted: ST " + msg.startTimestamp + " MAX "
                     + sharedState.largestDeletedTimestamp);
            return;
        }
        if (!sharedState.uncommited.isUncommitted(msg.startTimestamp)) {
            long commitTS = sharedState.hashmap.getCommittedTimestamp(msg.startTimestamp);
            if (commitTS == 0) {
                LOG.error("Transaction " + msg.startTimestamp + " has already been aborted");
            } else {
                LOG.error("Transaction " + msg.startTimestamp + " has already been committed with ts " + commitTS);
            }
            return; // TODO something better to do?
        }
        wal.logEvent(LoggerProtocol.ABORT, msg.startTimestamp);

        metrics.selfAborted();
        sharedState.processAbort(msg.startTimestamp);

        queueHalfAbort(msg.startTimestamp);
    }

    /**
     * Handle the TimestampRequest message
     */
    public void handleTimestamp(TimestampRequest msg, ChannelHandlerContext ctx) {
        metrics.begin();
        TimerContext timer = metrics.startBeginProcessing();
        long timestamp;

        try {
            timestamp = timestampOracle.next(wal);
            sharedState.uncommited.start(timestamp);
        } catch (IOException e) {
            LOG.error("Error getting timestamp", e);
            return;
        }

        long seq = sharedBufRing.next();
        SharedBufEvent event = sharedBufRing.get(seq);
        event.setType(SharedBufEvent.Type.START_TIMESTAMP).setContext(ctx)
            .setStartTimestamp(timestamp);
        sharedBufRing.publish(seq);

        timer.stop();
    }


    /**
     * Handle the CommitRequest message (very long method, can hotspot deal?)
     */
    public void handleCommit(CommitRequest msg, ChannelHandlerContext ctx) {
        TimerContext timerProcessing = metrics.startCommitProcessing();
        TimerContext timerLatency = metrics.startCommitLatency();
        
        boolean committed = false;
        long startTimestamp = msg.startTimestamp;
        long commitTimestamp = 0L;
        
        // 0. check if it should abort
        if (msg.startTimestamp < timestampOracle.first()) {
            committed = false;
            LOG.warn("Aborting transaction after restarting TSO");
        } else if (msg.startTimestamp <= sharedState.largestDeletedTimestamp) {
            committed = false;
            LOG.warn("Too old starttimestamp: ST " + msg.startTimestamp + " MAX "
                     + sharedState.largestDeletedTimestamp);
        } else if (!sharedState.uncommited.isUncommitted(msg.startTimestamp)) {
            long commitTS = sharedState.hashmap.getCommittedTimestamp(msg.startTimestamp);
            if (commitTS == 0) {
                LOG.error("Transaction " + msg.startTimestamp + " has already been aborted");
            } else {
                LOG.error("Transaction " + msg.startTimestamp + " has already been committed with ts " + commitTS);
            }
            
            return; // FIXME, just hangs the client right now
        } else {
            // 1. check the write-write conflicts
            committed = true;
            for (RowKey r : msg.rows) {
                long value;
                value = sharedState.hashmap.getLatestWriteForRow(r.hashCode());
                if (value != 0 && value > msg.startTimestamp) {
                    committed = false;
                    break;
                } else if (value == 0 && sharedState.largestDeletedTimestamp > msg.startTimestamp) {
                    // then it could have been committed after start
                    // timestamp but deleted by recycling
                    LOG.warn("Old transaction {Start timestamp  [{}], Largest deleted timestamp [{}]",                                 msg.startTimestamp,sharedState.largestDeletedTimestamp );
                    committed = false;
                    break;
                }
            }
        }

        if (committed) {
            metrics.commited();
            // 2. commit
            try {
                commitTimestamp = timestampOracle.next(wal);
                sharedState.uncommited.commit(msg.startTimestamp);

                if (msg.rows.length > 0) {
                    wal.logEvent(LoggerProtocol.COMMIT, msg.startTimestamp, commitTimestamp);

                    long largestDeletedTimestamp = sharedState.largestDeletedTimestamp;

                    for (RowKey r : msg.rows) {
                        long removed = sharedState.hashmap.putLatestWriteForRow(r.hashCode(), commitTimestamp);
                        largestDeletedTimestamp = Math.max(removed, largestDeletedTimestamp);
                    }

                    long removed = sharedState.processCommit(msg.startTimestamp, commitTimestamp);
                    largestDeletedTimestamp = Math.max(removed, largestDeletedTimestamp);
                    if (largestDeletedTimestamp > sharedState.largestDeletedTimestamp) {
                        sharedState.largestDeletedTimestamp = largestDeletedTimestamp;
                        handleLargestDeletedTimestampIncrease();
                    }
                    queueCommit(msg.startTimestamp, commitTimestamp);
                }
            } catch (IOException e) {
                LOG.error("Error committing", e);
            }
        } else { // add it to the aborted list
            metrics.aborted();

            wal.logEvent(LoggerProtocol.ABORT, msg.startTimestamp);

            if (msg.startTimestamp >= sharedState.largestDeletedTimestamp) {
                // otherwise it is already on aborted list
                sharedState.processAbort(msg.startTimestamp);

                queueHalfAbort(msg.startTimestamp);
            }
        }

        wal.queueReponse(ctx, committed, startTimestamp, commitTimestamp);

        timerProcessing.stop();
        timerLatency.stop();
    }

    /**
     * Handle the FullAbortReport message
     */
    public void handleFullAbort(FullAbortRequest msg, ChannelHandlerContext ctx) {
        long timestamp = msg.startTimestamp;

        wal.logEvent(LoggerProtocol.FULLABORT, msg.startTimestamp);

        metrics.cleanedAbort();

        sharedState.processFullAbort(timestamp);

        queueFullAbort(timestamp);
    }

    /**
     * Handle the CommitQueryRequest message
     */
    public void handleCommitQuery(CommitQueryRequest msg, ChannelHandlerContext ctx) {
        metrics.query();
        long seq = responseRing.next();
        ReplyEvent event = responseRing.get(seq);

        event.setType(ReplyEvent.Type.COMMIT_QUERY).setContext(ctx)
            .setStartTimestamp(msg.startTimestamp).setQueryTimestamp(msg.queryTimestamp);

        // 1. check the write-write conflicts
        long value;
        value = sharedState.hashmap.getCommittedTimestamp(msg.queryTimestamp);
        event.setCommitTimestamp(0);
        event.setRetry(false);
        if (value != 0) { // it exists
            event.setCommitTimestamp(value).setCommitted(value < msg.startTimestamp);// set as abort
        } else if (sharedState.hashmap.isHalfAborted(msg.queryTimestamp)) {
            event.setCommitted(false);
        } else if (sharedState.uncommited.isUncommitted(msg.queryTimestamp)) {
            event.setCommitted(false);
        } else {
            event.setRetry(true);
        }
        responseRing.publish(seq);
    }

    public void createAbortedSnapshot() {
        long snapshot = sharedState.hashmap.getAndIncrementAbortedSnapshot();

        wal.logEvent(LoggerProtocol.SNAPSHOT, snapshot);
        for (AbortedTransaction aborted : sharedState.hashmap.halfAborted) {
            // ignore aborted transactions from last snapshot
            if (aborted.getSnapshot() < snapshot) {
                wal.logEvent(LoggerProtocol.ABORT, aborted.getStartTimestamp());
            }
        }
    }


    private void handleLargestDeletedTimestampIncrease() throws IOException {
        wal.logEvent(LoggerProtocol.LARGESTDELETEDTIMESTAMP, sharedState.largestDeletedTimestamp);
        Set<Long> toAbort = sharedState.uncommited.raiseLargestDeletedTransaction(sharedState.largestDeletedTimestamp);
        if (LOG.isWarnEnabled() && !toAbort.isEmpty()) {
            LOG.warn("Slow transactions after raising max: " + toAbort.size());
        }
        metrics.oldAborted(toAbort.size());

        for (Long id : toAbort) {
            sharedState.addExistingAbort(id);
            queueHalfAbort(id);
        }
        queueLargestIncrease(sharedState.largestDeletedTimestamp);

        if (sharedState.largestDeletedTimestamp > sharedState.previousLargestDeletedTimestamp
                + TSOState.MAX_ITEMS) {
            // schedule snapshot
            createAbortedSnapshot();
            sharedState.previousLargestDeletedTimestamp = sharedState.largestDeletedTimestamp;
        }
    }

    private void queueCommit(long startTimestamp, long commitTimestamp) {
        long seq = sharedBufRing.next();
        SharedBufEvent event = sharedBufRing.get(seq);
        event.setType(SharedBufEvent.Type.COMMIT);
        event.setStartTimestamp(startTimestamp);
        event.setCommitTimestamp(commitTimestamp);
        sharedBufRing.publish(seq);
    }

    private void queueHalfAbort(long startTimestamp) {
        long seq = sharedBufRing.next();
        SharedBufEvent event = sharedBufRing.get(seq);
        event.setType(SharedBufEvent.Type.HALF_ABORT);
        event.setStartTimestamp(startTimestamp);
        sharedBufRing.publish(seq);
    }

    private void queueFullAbort(long startTimestamp) {
        long seq = sharedBufRing.next();
        SharedBufEvent event = sharedBufRing.get(seq);
        event.setType(SharedBufEvent.Type.FULL_ABORT);
        event.setStartTimestamp(startTimestamp);
        sharedBufRing.publish(seq);
    }

    private void queueLargestIncrease(long largestTimestamp) {
        long seq = sharedBufRing.next();
        SharedBufEvent event = sharedBufRing.get(seq);
        event.setType(SharedBufEvent.Type.LARGEST_INCREASE);
        event.setLargestTimestamp(largestTimestamp);
        sharedBufRing.publish(seq);
    }
};
