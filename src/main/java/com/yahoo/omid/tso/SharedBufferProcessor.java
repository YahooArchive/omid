package com.yahoo.omid.tso;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;

import com.yahoo.omid.replication.SharedMessageBuffer.ReadingBuffer;
import com.yahoo.omid.replication.SharedMessageBuffer;

import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.messages.TimestampResponse;

import com.yahoo.omid.tso.CompacterHandler.CompactionEvent;

import com.yahoo.omid.tso.metrics.StatusOracleMetrics;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SharedBufferProcessor implements EventHandler<SharedBufferProcessor.SharedBufEvent>
{
    private static final Logger LOG = LoggerFactory.getLogger(SharedBufferProcessor.class);

    public SharedMessageBuffer sharedMessageBuffer = new SharedMessageBuffer();
    private Map<Channel, ReadingBuffer> messageBuffersMap = new HashMap<Channel, ReadingBuffer>();

    long largestDeletedTimestamp = 0;
    Set<Long> halfAborted = new HashSet<Long>(10000);
    final RingBuffer<CompactionEvent> compactionRing;

    StatusOracleMetrics metrics = new StatusOracleMetrics();

    SharedBufferProcessor(RingBuffer<CompactionEvent> compactionRing) {
        this.compactionRing = compactionRing;
    }

    public void onEvent(final SharedBufEvent event, final long sequence, final boolean endOfBatch)
        throws Exception
    {
        switch (event.getType()) {
        case START_TIMESTAMP:
            handleStartTimestamp(event);
            break;
        case CHANNEL_CLOSED:
            handleChannelClosed(event);
            break;
        case COMMIT:
            handleCommit(event);
            break;
        case HALF_ABORT:
            handleHalfAbort(event);
            break;
        case FULL_ABORT:
            handleFullAbort(event);
            break;
        case LARGEST_INCREASE:
            handleLargestDeletedIncrease(event);
            break;
        default:
            LOG.error("Unknown event {}", event.getType());
            break;
        }
    }

    private void handleCommit(SharedBufEvent event) {
        sharedMessageBuffer.writeCommit(event.getStartTimestamp(), event.getCommitTimestamp());
    }

    private void handleHalfAbort(SharedBufEvent event) {
        halfAborted.add(event.getStartTimestamp());
        sharedMessageBuffer.writeHalfAbort(event.getStartTimestamp());
    }

    private void handleFullAbort(SharedBufEvent event) {
        halfAborted.remove(event.getStartTimestamp());
        sharedMessageBuffer.writeFullAbort(event.getStartTimestamp());
    }

    private void handleLargestDeletedIncrease(SharedBufEvent event) {
        largestDeletedTimestamp = event.getLargestTimestamp();
        sharedMessageBuffer.writeLargestIncrease(event.getLargestTimestamp());

        long seq = compactionRing.next();
        CompactionEvent compEvent = compactionRing.get(seq);
        compEvent.setLargestDeletedTimestamp(largestDeletedTimestamp);
        compactionRing.publish(seq);
    }

    private void handleStartTimestamp(SharedBufEvent event) {
        Channel channel = event.getContext().getChannel();

        ReadingBuffer buffer = messageBuffersMap.get(event.getContext().getChannel());
        if (buffer == null) {
            buffer = sharedMessageBuffer.getReadingBuffer(event.getContext());
            messageBuffersMap.put(channel, buffer);
            metrics.getConnectionCounter().inc();
            channel.write(buffer.getZipperState());
            buffer.initializeIndexes();

            channel.write(new LargestDeletedTimestampReport(largestDeletedTimestamp));
            Iterator<Long> iter = halfAborted.iterator();
            while (iter.hasNext()) {
                channel.write(new AbortedTransactionReport(iter.next()));
            }
        }

        ChannelFuture future = Channels.future(channel);
        ChannelBuffer cb = buffer.flush(future);
        Channels.write(event.getContext(), future, cb);
        Channels.write(channel, new TimestampResponse(event.getStartTimestamp()));
    }
   
    private void handleChannelClosed(SharedBufEvent event) {
        sharedMessageBuffer.removeReadingBuffer(event.getContext());
        metrics.getConnectionCounter().dec();
    }

    public final static class SharedBufEvent {
        enum Type {
            START_TIMESTAMP, CHANNEL_CLOSED,
            COMMIT, HALF_ABORT, FULL_ABORT, LARGEST_INCREASE, NULL
        }
        private Type type = null;
        private long startTimestamp = 0;
        private long commitTimestamp = 0;
        private long largestTimestamp = 0;
       
        private ChannelHandlerContext ctx = null;
        private long largestDeletedTimestamp = 0;
        private long halfAbortedTimestamp = 0;

        Type getType() { return type; }
        SharedBufEvent setType(Type type) {
            this.type = type;
            return this;
        }
        long getStartTimestamp() { return startTimestamp; }
        SharedBufEvent setStartTimestamp(long startTimestamp) {
            this.startTimestamp = startTimestamp;
            return this;
        }
        long getCommitTimestamp() { return commitTimestamp; }
        SharedBufEvent setCommitTimestamp(long commitTimestamp) {
            this.commitTimestamp = commitTimestamp;
            return this;
        }
        long getLargestTimestamp() { return largestTimestamp; }
        SharedBufEvent setLargestTimestamp(long largestTimestamp) {
            this.largestTimestamp = largestTimestamp;
            return this;
        }

        ChannelHandlerContext getContext() { return ctx; }
        SharedBufEvent setContext(ChannelHandlerContext ctx) {
            this.ctx = ctx;
            return this;
        }
        long getLargestDeletedTimestamp() { return largestDeletedTimestamp; }
        SharedBufEvent setLargestDeletedTimestamp(long largestDeletedTimestamp) {
            this.largestDeletedTimestamp = largestDeletedTimestamp;
            return this;
        }
        long getHalfAbortedTimestamp() { return halfAbortedTimestamp; }
        SharedBufEvent setHalfAbortedTimestamp(long halfAbortedTimestamp) {
            this.halfAbortedTimestamp = halfAbortedTimestamp;
            return this;
        }

        public final static EventFactory<SharedBufEvent> EVENT_FACTORY
            = new EventFactory<SharedBufEvent>()
        {
            public SharedBufEvent newInstance()
            {
                return new SharedBufEvent();
            }
        };
    }
}

