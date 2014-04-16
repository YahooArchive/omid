package com.yahoo.omid.tso;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import java.util.ArrayList;
import java.util.List;
import java.util.ArrayList;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;

import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerException;
import com.yahoo.omid.tso.persistence.LoggerException.Code;
import com.yahoo.omid.tso.persistence.StateLogger;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PersistenceProcessorImpl implements EventHandler<PersistenceProcessorImpl.WALEvent>, PersistenceProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessor.class);

    private final static byte QUEUE_RESPONSE = -16;

    ByteArrayOutputStream buffer = new ByteArrayOutputStream(TSOState.BATCH_SIZE);
    DataOutputStream dataBuffer = new DataOutputStream(buffer);
    final StateLogger logger;
    final ReplyProcessor reply;
    
    PersistenceProcessorImpl(ReplyProcessor reply) {
        this.logger = null;
        this.reply = reply;

        RingBuffer<WALEvent> walRing = RingBuffer.<WALEvent>createSingleProducer(WALEvent.EVENT_FACTORY, 1<<12,
                                                                                 new BusySpinWaitStrategy());
        SequenceBarrier walSequenceBarrier = walRing.newBarrier();
        BatchEventProcessor<WALEvent> walProcessor = new BatchEventProcessor<WALEvent>(
                walRing,
                walSequenceBarrier,
                this);
        walRing.addGatingSequences(walProcessor.getSequence());

        ExecutorService walExec = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("persist-%d").build());
        walExec.submit(walProcessor);
    }

    @Override
    public void onEvent(final WALEvent event, final long sequence, final boolean endOfBatch)
        throws Exception {
        /*if (event.getOp() == QUEUE_RESPONSE) {
            CommitResponse msg = new CommitResponse(event.getParam(1));
            msg.committed = (event.getParam(0) == 1);
            msg.commitTimestamp = event.getParam(2);
            
            deferredResponses.add(new DeferredResponse(event.getContext().getChannel(), msg));
        } else {
            dataBuffer.writeByte(event.getOp());
            for (int i = 0; i < event.getNumParams(); i++) {
                dataBuffer.writeLong(event.getParam(i));
            }
        }
        
        if (endOfBatch || buffer.size() > TSOState.BATCH_SIZE) {
            flush();
            }*/
    }
    
    @Override
    public void persistCommit(long startTimestamp, long commitTimestamp, Channel c) {
        reply.commitResponse(startTimestamp, commitTimestamp, c);
    }
    
    @Override
    public void persistAbort(long startTimestamp, Channel c) {
        reply.abortResponse(startTimestamp, c);
    }

    @Override
    public void persistTimestamp(long startTimestamp, Channel c) {
        reply.timestampResponse(startTimestamp, c);
    }

    public final static class WALEvent {
        final static int MAX_PARAMS = 8;
        private byte op;
        private int numParam = 0;
        private long[] params = new long[MAX_PARAMS];
                
        private ChannelHandlerContext ctx = null;
        
        ChannelHandlerContext getContext() { return ctx; }
        WALEvent setContext(ChannelHandlerContext ctx) {
            this.ctx = ctx;
            return this;
        }

        byte getOp() { return op; }
        WALEvent setOp(byte op) {
            this.op = op;
            return this;
        }

        long getParam(int index) {
            assert (index < numParam && index >= 0);
            return params[index];            
        }
        WALEvent setParam(int index, long value) {
            assert (index < numParam && index >= 0);
            params[index] = value;
            return this;
        }

        int getNumParams() {
            return numParam;
        }
        WALEvent setNumParams(int numParam) {
            assert (numParam >= 0 && numParam <= MAX_PARAMS);
            this.numParam = numParam;
            return this;
        }

        public final static EventFactory<WALEvent> EVENT_FACTORY
            = new EventFactory<WALEvent>() {
            public WALEvent newInstance()
            {
                return new WALEvent();
            }
        };
    }

}
