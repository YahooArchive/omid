/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.AdaptiveReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.util.ObjectSizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.metrics.MetricsUtils;
import com.yahoo.omid.tso.persistence.BookKeeperStateBuilder;
import com.yahoo.omid.tso.persistence.FileSystemTimestampOnlyStateBuilder;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerProtocol;

import com.yahoo.omid.tso.ReplyProcessor.ReplyEvent;
import com.yahoo.omid.tso.WALProcessor.WALEvent;
import com.yahoo.omid.tso.CompacterHandler.CompactionEvent;
import com.yahoo.omid.tso.RequestProcessor.RequestEvent;

import com.lmax.disruptor.*;

/**
 * TSO Server
 */
public class TSOServer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TSOServer.class);

    private TSOServerConfig config;
    private boolean finish = false;
    private final Object lock = new Object();

    public TSOServer() {
        super();
        this.config = TSOServerConfig.configFactory();
    }

    public TSOServer(TSOServerConfig config) {
        super();
        this.config = config;
    }

    public static void main(String[] args) throws Exception {
        TSOServerConfig config = TSOServerConfig.parseConfig(args);

        new TSOServer(config).run();
    }

    @Override
    public void run() {
        String metricsConfig = config.getMetrics();
        if (metricsConfig != null) {
            MetricsUtils.initMetrics(metricsConfig);
        }

        RingBuffer<RequestEvent> requestRing = RingBuffer.<RequestEvent>createMultiProducer(RequestEvent.EVENT_FACTORY, 1<<12,
                                                                                            new BusySpinWaitStrategy());
        RingBuffer<WALEvent> walRing = RingBuffer.<WALEvent>createSingleProducer(WALEvent.EVENT_FACTORY, 1<<12,
                                                                                 new BusySpinWaitStrategy());
        RingBuffer<ReplyEvent> replyRing = RingBuffer.<ReplyEvent>createMultiProducer(ReplyEvent.EVENT_FACTORY, 1<<12,
                                                                                      new BusySpinWaitStrategy());
        SequenceBarrier replySequenceBarrier = replyRing.newBarrier();
        ReplyProcessor reply = new ReplyProcessor();
        BatchEventProcessor<ReplyEvent> replyProcessor = new BatchEventProcessor<ReplyEvent>(
                replyRing,
                replySequenceBarrier,
                reply);
        replyRing.addGatingSequences(replyProcessor.getSequence());

        SequenceBarrier walSequenceBarrier = walRing.newBarrier();
        WALProcessor wal = new WALProcessor(walRing, reply);
        BatchEventProcessor<WALEvent> walProcessor = new BatchEventProcessor<WALEvent>(
                walRing,
                walSequenceBarrier,
                wal);
        walRing.addGatingSequences(walProcessor.getSequence());

        SequenceBarrier requestSequenceBarrier = requestRing.newBarrier();
        BatchEventProcessor requestProcessor = new BatchEventProcessor<RequestEvent>(
                requestRing,
                requestSequenceBarrier,
                new RequestProcessor(0L, wal, reply)); // FIXME first timestamp should be loaded from WAL
        requestRing.addGatingSequences(requestProcessor.getSequence());

        ExecutorService requestExec = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("request-%d").build());
        ExecutorService walExec = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("wal-%d").build());
        ExecutorService replyExec = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("reply-%d").build());

        // Each processor runs on a separate thread
        requestExec.submit(requestProcessor);
        replyExec.submit(replyProcessor);
        walExec.submit(walProcessor);

        // Setup netty listener
        ChannelFactory factory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setNameFormat("boss-%d").build()),
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setNameFormat("worker-%d").build()),
                (Runtime.getRuntime().availableProcessors() * 2 + 1) * 2);

        // Create the global ChannelGroup
        ChannelGroup channelGroup = new DefaultChannelGroup(TSOServer.class.getName());

        final TSOHandler handler = new TSOHandler(channelGroup, requestRing);

        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        bootstrap.setPipelineFactory(new TSOPipelineFactory(handler));

        // Add the parent channel to the group
        Channel channel = bootstrap.bind(new InetSocketAddress(config.getPort()));
        channelGroup.add(channel);

        synchronized (lock) {
            while (!finish) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        // *** Start the Netty shutdown ***
        // Now close all channels
        LOG.info("End of channel group");
        channelGroup.close().awaitUninterruptibly();

        // Now release resources
        LOG.info("End of resources");
        factory.releaseExternalResources();
    }

    public void stop() {
        finish = true;
        synchronized (lock) {
            lock.notifyAll();
        }
    }
}
