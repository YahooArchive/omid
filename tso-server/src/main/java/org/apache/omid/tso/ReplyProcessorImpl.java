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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import org.apache.omid.metrics.Meter;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.proto.TSOProto;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.omid.metrics.MetricsUtils.name;

class ReplyProcessorImpl implements EventHandler<ReplyProcessorImpl.ReplyEvent>, ReplyProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ReplyProcessorImpl.class);

    final RingBuffer<ReplyEvent> replyRing;
    final Meter abortMeter;
    final Meter commitMeter;
    final Meter timestampMeter;

    @Inject
    ReplyProcessorImpl(MetricsRegistry metrics, Panicker panicker) {
        replyRing = RingBuffer.createMultiProducer(ReplyEvent.EVENT_FACTORY, 1 << 12, new BusySpinWaitStrategy());
        SequenceBarrier replySequenceBarrier = replyRing.newBarrier();
        BatchEventProcessor<ReplyEvent> replyProcessor = new BatchEventProcessor<ReplyEvent>(
                replyRing, replySequenceBarrier, this);
        replyProcessor.setExceptionHandler(new FatalExceptionHandler(panicker));

        replyRing.addGatingSequences(replyProcessor.getSequence());

        ExecutorService replyExec = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("reply-%d").build());
        replyExec.submit(replyProcessor);

        abortMeter = metrics.meter(name("tso", "aborts"));
        commitMeter = metrics.meter(name("tso", "commits"));
        timestampMeter = metrics.meter(name("tso", "timestampAllocation"));
    }

    public void onEvent(ReplyEvent event, long sequence, boolean endOfBatch) throws Exception {
        String name = null;
        try {
            switch (event.getType()) {
                case COMMIT:
                    name = "commitReplyProcessor";
                    event.getMonCtx().timerStart(name);
                    handleCommitResponse(false, event.getStartTimestamp(), event.getCommitTimestamp(), event.getChannel());
                    break;
                case HEURISTIC_COMMIT:
                    name = "commitReplyProcessor";
                    event.getMonCtx().timerStart(name);
                    handleCommitResponse(true, event.getStartTimestamp(), event.getCommitTimestamp(), event.getChannel());
                    break;
                case ABORT:
                    name = "abortReplyProcessor";
                    event.getMonCtx().timerStart(name);
                    handleAbortResponse(event.getStartTimestamp(), event.getChannel());
                    break;
                case TIMESTAMP:
                    name = "timestampReplyProcessor";
                    event.getMonCtx().timerStart(name);
                    handleTimestampResponse(event.getStartTimestamp(), event.getChannel());
                    break;
                default:
                    LOG.error("Unknown event {}", event.getType());
                    break;
            }
        } finally {
            if (name != null) {
                event.getMonCtx().timerStop(name);
            }
        }
        event.getMonCtx().publish();
    }

    @Override
    public void commitResponse(boolean makeHeuristicDecision, long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx) {
        long seq = replyRing.next();
        ReplyEvent e = replyRing.get(seq);
        ReplyEvent.makeCommitResponse(makeHeuristicDecision, e, startTimestamp, commitTimestamp, c, monCtx);
        replyRing.publish(seq);
    }

    @Override
    public void abortResponse(long startTimestamp, Channel c, MonitoringContext monCtx) {
        long seq = replyRing.next();
        ReplyEvent e = replyRing.get(seq);
        ReplyEvent.makeAbortResponse(e, startTimestamp, c, monCtx);
        replyRing.publish(seq);
    }

    @Override
    public void timestampResponse(long startTimestamp, Channel c, MonitoringContext monCtx) {
        long seq = replyRing.next();
        ReplyEvent e = replyRing.get(seq);
        ReplyEvent.makeTimestampReponse(e, startTimestamp, c, monCtx);
        replyRing.publish(seq);
    }

    void handleCommitResponse(boolean makeHeuristicDecision, long startTimestamp, long commitTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        if (makeHeuristicDecision) { // If the commit is ambiguous is due to a new master TSO
            commitBuilder.setMakeHeuristicDecision(true);
        }
        commitBuilder.setAborted(false)
                .setStartTimestamp(startTimestamp)
                .setCommitTimestamp(commitTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.write(builder.build());

        commitMeter.mark();
    }

    void handleAbortResponse(long startTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        commitBuilder.setAborted(true)
                .setStartTimestamp(startTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.write(builder.build());

        abortMeter.mark();
    }

    void handleTimestampResponse(long startTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.TimestampResponse.Builder respBuilder = TSOProto.TimestampResponse.newBuilder();
        respBuilder.setStartTimestamp(startTimestamp);
        builder.setTimestampResponse(respBuilder.build());
        c.write(builder.build());

        timestampMeter.mark();
    }

    public final static class ReplyEvent {

        enum Type {
            TIMESTAMP, COMMIT, HEURISTIC_COMMIT, ABORT
        }

        private Type type = null;
        private Channel channel = null;

        private long startTimestamp = 0;
        private long commitTimestamp = 0;
        private MonitoringContext monCtx;

        Type getType() {
            return type;
        }

        Channel getChannel() {
            return channel;
        }

        long getStartTimestamp() {
            return startTimestamp;
        }

        long getCommitTimestamp() {
            return commitTimestamp;
        }

        MonitoringContext getMonCtx() {
            return monCtx;
        }

        static void makeTimestampReponse(ReplyEvent e, long startTimestamp, Channel c, MonitoringContext monCtx) {
            e.type = Type.TIMESTAMP;
            e.startTimestamp = startTimestamp;
            e.channel = c;
            e.monCtx = monCtx;
        }

        static void makeCommitResponse(boolean makeHeuristicDecision, ReplyEvent e, long startTimestamp,
                                       long commitTimestamp, Channel c, MonitoringContext monCtx) {

            if (makeHeuristicDecision) {
                e.type = Type.HEURISTIC_COMMIT;
            } else {
                e.type = Type.COMMIT;
            }
            e.startTimestamp = startTimestamp;
            e.commitTimestamp = commitTimestamp;
            e.channel = c;
            e.monCtx = monCtx;
        }

        static void makeAbortResponse(ReplyEvent e, long startTimestamp, Channel c, MonitoringContext monCtx) {
            e.type = Type.ABORT;
            e.startTimestamp = startTimestamp;
            e.channel = c;
            e.monCtx = monCtx;
        }

        public final static EventFactory<ReplyEvent> EVENT_FACTORY = new EventFactory<ReplyEvent>() {
            @Override
            public ReplyEvent newInstance() {
                return new ReplyEvent();
            }
        };

    }

}

