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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.notifications.metrics.MetricsUtils;
import com.yahoo.omid.tso.persistence.BookKeeperStateBuilder;
import com.yahoo.omid.tso.persistence.FileSystemTimestampOnlyStateBuilder;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.yahoo.omid.tso.persistence.LoggerProtocol;

/**
 * TSO Server with serialization
 */
public class TSOServer implements Runnable {

    private static final Log LOG = LogFactory.getLog(BookKeeperStateBuilder.class);

    private TSOState state;
    private TSOServerConfig config;
    private boolean finish;
    private Object lock;

    public TSOServer() {
        super();
        this.config = TSOServerConfig.configFactory();

        this.finish = false;
        this.lock = new Object();
    }

    public TSOServer(TSOServerConfig config) {
        super();
        this.config = config;

        this.finish = false;
        this.lock = new Object();
    }

    public TSOState getState() {
        return state;
    }

    /**
     * Take two arguments :<br>
     * -port to listen to<br>
     * -nb of connections before shutting down
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        TSOServerConfig config = TSOServerConfig.parseConfig(args);

        new TSOServer(config).run();
    }

    @Override
    public void run() {
        // *** Start the Netty configuration ***
        // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
        ChannelFactory factory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("boss-%d").build()),
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("worker-%d").build()), (Runtime
                        .getRuntime().availableProcessors() * 2 + 1) * 2);

        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        // Create the global ChannelGroup
        ChannelGroup channelGroup = new DefaultChannelGroup(TSOServer.class.getName());
        // threads max
        // int maxThreads = Runtime.getRuntime().availableProcessors() *2 + 1;
        int maxThreads = 5;
        // Memory limitation: 1MB by channel, 1GB global, 100 ms of timeout
        ThreadPoolExecutor pipelineExecutor = new OrderedMemoryAwareThreadPoolExecutor(maxThreads, 1048576, 1073741824,
                100, TimeUnit.MILLISECONDS, new ObjectSizeEstimator() {
                    @Override
                    public int estimateSize(Object o) {
                        return 1000;
                    }
                }, new ThreadFactoryBuilder().setNameFormat("executor-%d").build());

        // TODO use dependency injection
        if (config.getFsLog() != null) {
            state = FileSystemTimestampOnlyStateBuilder.getState(config);
        } else {
            state = BookKeeperStateBuilder.getState(this.config);
        }

        if (state == null) {
            LOG.error("Couldn't build state");
            return;
        }

        state.addRecord(new byte[] { LoggerProtocol.LOGSTART }, new AddRecordCallback() {
            @Override
            public void addRecordComplete(int rc, Object ctx) {
            }
        }, null);

        String metricsConfig = config.getMetrics();
        if (metricsConfig != null) {
            MetricsUtils.initMetrics(metricsConfig);
        }

        TSOState.BATCH_SIZE = config.getBatchSize();
        System.out.println("PARAM MAX_ITEMS: " + TSOState.MAX_ITEMS);
        System.out.println("PARAM BATCH_SIZE: " + TSOState.BATCH_SIZE);
        System.out.println("PARAM LOAD_FACTOR: " + TSOState.LOAD_FACTOR);
        System.out.println("PARAM MAX_THREADS: " + maxThreads);

        final TSOHandler handler = new TSOHandler(channelGroup, state);
        handler.start();

        bootstrap.setPipelineFactory(new TSOPipelineFactory(pipelineExecutor, handler));
        bootstrap.setOption("tcpNoDelay", true);
        // setting buffer size can improve I/O
        bootstrap.setOption("child.sendBufferSize", 1048576);
        bootstrap.setOption("child.receiveBufferSize", 1048576);
        // better to have an receive buffer predictor
        bootstrap.setOption("receiveBufferSizePredictorFactory", new AdaptiveReceiveBufferSizePredictorFactory());
        // if the server is sending 1000 messages per sec, optimum write buffer water marks will
        // prevent unnecessary throttling, Check NioSocketChannelConfig doc
        bootstrap.setOption("writeBufferLowWaterMark", 32 * 1024);
        bootstrap.setOption("writeBufferHighWaterMark", 64 * 1024);

        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.reuseAddress", true);
        bootstrap.setOption("child.connectTimeoutMillis", 60000);

        // *** Start the Netty running ***

        // Add the parent channel to the group
        Channel channel = bootstrap.bind(new InetSocketAddress(config.getPort()));
        channelGroup.add(channel);

        // Compacter handler
        ChannelFactory comFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("compacter-boss-%d").build()),
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("compacter-worker-%d").build()),
                (Runtime.getRuntime().availableProcessors() * 2 + 1) * 2);
        ServerBootstrap comBootstrap = new ServerBootstrap(comFactory);
        ChannelGroup comGroup = new DefaultChannelGroup("compacter");
        final CompacterHandler comHandler = new CompacterHandler(comGroup, state);
        comBootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("decoder", new ObjectDecoder());
                pipeline.addLast("encoder", new ObjectEncoder());
                pipeline.addLast("handler", comHandler);
                return pipeline;
            }
        });
        comBootstrap.setOption("tcpNoDelay", true);
        comBootstrap.setOption("child.tcpNoDelay", true);
        comBootstrap.setOption("child.keepAlive", true);
        comBootstrap.setOption("child.reuseAddress", true);
        comBootstrap.setOption("child.connectTimeoutMillis", 100);
        comBootstrap.setOption("readWriteFair", true);
        channel = comBootstrap.bind(new InetSocketAddress(config.getPort() + 1));

        synchronized (lock) {
            while (!finish) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        // timestampOracle.stop();
        handler.stop();
        comHandler.stop();
        state.stop();
        state = null;

        // *** Start the Netty shutdown ***

        // Now close all channels
        System.out.println("End of channel group");
        channelGroup.close().awaitUninterruptibly();
        comGroup.close().awaitUninterruptibly();
        // Close the executor for Pipeline
        System.out.println("End of pipeline executor");
        pipelineExecutor.shutdownNow();
        // Now release resources
        System.out.println("End of resources");
        factory.releaseExternalResources();
        comFactory.releaseExternalResources();
        comBootstrap.releaseExternalResources();
    }

    public void stop() {
        finish = true;
        synchronized (lock) {
            lock.notifyAll();
        }
    }
}
