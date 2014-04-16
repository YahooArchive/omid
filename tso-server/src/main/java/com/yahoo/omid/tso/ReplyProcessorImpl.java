package com.yahoo.omid.tso;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;

import com.yahoo.omid.proto.TSOProto;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReplyProcessorImpl implements EventHandler<ReplyProcessorImpl.ReplyEvent>, ReplyProcessor
{
    private static final Logger LOG = LoggerFactory.getLogger(ReplyProcessorImpl.class);

    final RingBuffer<ReplyEvent> replyRing;

    ReplyProcessorImpl() {
        replyRing = RingBuffer.<ReplyEvent>createMultiProducer(ReplyEvent.EVENT_FACTORY, 1<<12,
                                                               new BusySpinWaitStrategy());
        SequenceBarrier replySequenceBarrier = replyRing.newBarrier();
        BatchEventProcessor<ReplyEvent> replyProcessor = new BatchEventProcessor<ReplyEvent>(
                                                                                             replyRing,
                                                                                             replySequenceBarrier,
                                                                                             this);
        replyRing.addGatingSequences(replyProcessor.getSequence());

        ExecutorService replyExec = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("reply-%d").build());
        replyExec.submit(replyProcessor);
    }

    public void onEvent(final ReplyEvent event, final long sequence, final boolean endOfBatch)
        throws Exception
    {
        switch (event.getType()) {
        case COMMIT:
            handleCommitResponse(event.getStartTimestamp(), event.getCommitTimestamp(), event.getChannel());
            break;
        case ABORT:
            handleAbortResponse(event.getStartTimestamp(), event.getChannel());
            break;
        case TIMESTAMP:
            handleTimestampResponse(event.getStartTimestamp(), event.getChannel());
            break;
        default:
            LOG.error("Unknown event {}", event.getType());
            break;
        }
    }

    @Override
    public void commitResponse(long startTimestamp, long commitTimestamp, Channel c) {
        long seq = replyRing.next();
        ReplyEvent e = replyRing.get(seq);
        ReplyEvent.makeCommitResponse(e, startTimestamp, commitTimestamp, c);
        replyRing.publish(seq);
    }

    @Override
    public void abortResponse(long startTimestamp, Channel c) {
        long seq = replyRing.next();
        ReplyEvent e = replyRing.get(seq);
        ReplyEvent.makeAbortResponse(e, startTimestamp, c);
        replyRing.publish(seq);
    }

    @Override
    public void timestampResponse(long startTimestamp, Channel c) {
        long seq = replyRing.next();
        ReplyEvent e = replyRing.get(seq);
        ReplyEvent.makeTimestampReponse(e, startTimestamp, c);
        replyRing.publish(seq);
    }

    void handleCommitResponse(long startTimestamp, long commitTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        commitBuilder.setAborted(false)
            .setStartTimestamp(startTimestamp)
            .setCommitTimestamp(commitTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.write(builder.build());
    }

    void handleAbortResponse(long startTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        commitBuilder.setAborted(true)
            .setStartTimestamp(startTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.write(builder.build());
    }

    void handleTimestampResponse(long startTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.TimestampResponse.Builder respBuilder = TSOProto.TimestampResponse.newBuilder();
        respBuilder.setStartTimestamp(startTimestamp);
        builder.setTimestampResponse(respBuilder.build());
        c.write(builder.build());
    }

    public final static class ReplyEvent {
        enum Type {
            TIMESTAMP, COMMIT, ABORT
        }
        private Type type = null;
        private Channel channel = null;

        private long startTimestamp = 0;
        private long commitTimestamp = 0;
        
        Type getType() { return type; }
        Channel getChannel() { return channel; }
        long getStartTimestamp() { return startTimestamp; }
        long getCommitTimestamp() { return commitTimestamp; }

        static void makeTimestampReponse(ReplyEvent e, long startTimestamp, Channel c) {
            e.type = Type.TIMESTAMP;
            e.startTimestamp = startTimestamp;
            e.channel = c;
        }

        static void makeCommitResponse(ReplyEvent e, long startTimestamp, long commitTimestamp, Channel c) {
            e.type = Type.COMMIT;
            e.startTimestamp = startTimestamp;
            e.commitTimestamp = commitTimestamp;
            e.channel = c;
        }

        static void makeAbortResponse(ReplyEvent e, long startTimestamp, Channel c) {
            e.type = Type.ABORT;
            e.startTimestamp = startTimestamp;
            e.channel = c;
        }

        public final static EventFactory<ReplyEvent> EVENT_FACTORY
            = new EventFactory<ReplyEvent>()
        {
            public ReplyEvent newInstance()
            {
                return new ReplyEvent();
            }
        };
    }
}

