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
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.codahale.metrics.MetricRegistry;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.NullCommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.metrics.MetricsUtils;
import com.yahoo.omid.tso.TimestampOracle.TimestampStorage;
import com.yahoo.omid.tso.hbase.HBaseTimestampStorage;

import static com.yahoo.omid.tso.TimestampOracle.InMemoryTimestampStorage;

public class TSOServer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(TSOServer.class);

    private final MetricRegistry metrics;

    private final TimestampOracle timestampOracle;

    private final TSOServerConfig config;
    private final CommitTable commitTable;
    
    private boolean finish = false;
    private final Object lock = new Object();

    public TSOServer(TSOServerConfig config, MetricRegistry metrics, CommitTable commitTable, TimestampOracle timestampOracle) {
        this.config = config;
        this.metrics = metrics;
        this.timestampOracle = timestampOracle;
        this.commitTable = commitTable;
    }

    public static void main(String[] args) throws Exception {
        TSOServerConfig config = TSOServerConfig.parseConfig(args);

        if (config.hasHelpFlag()) {
            config.usage();
            return;
        }

        MetricRegistry metrics = MetricsUtils.initMetrics(config.getMetrics());

        CommitTable commitTable;
        TimestampStorage timestampStorage;
        if (config.isHBase()) {
            Configuration hbaseConfig = HBaseConfiguration.create();
            commitTable = new HBaseCommitTable(hbaseConfig , config.getHBaseCommitTable());
            HTable timestampHTable = new HTable(hbaseConfig , config.getHBaseTimestampTable());
            timestampStorage = new HBaseTimestampStorage(timestampHTable);
        } else {
            commitTable = new NullCommitTable();
            timestampStorage = new InMemoryTimestampStorage();
        }
        TimestampOracle timestampOracle = new TimestampOracle(metrics, timestampStorage);
        new TSOServer(config, metrics, commitTable, timestampOracle).run();
    }

    @Override
    public void run() {

        // Disruptor setup
        ReplyProcessor replyProc = new ReplyProcessorImpl(metrics);
        RetryProcessor retryProc;
        try {
            retryProc = new RetryProcessorImpl(metrics, commitTable, replyProc);
        } catch (ExecutionException ee) {
            LOG.error("Can't build the retry processor", ee);
            throw new IllegalStateException("Cannot run without a retry processor");
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted creating retry processor");
        }
        PersistenceProcessor persistProc;
        try {
            persistProc = new PersistenceProcessorImpl(metrics, commitTable, replyProc, retryProc, 
                    config.getMaxBatchSize());
        } catch (ExecutionException ee) {
            LOG.error("Can't build the persistence processor", ee);
            throw new IllegalStateException("Cannot run without a persist processor");
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted creating persist processor");
        }
        RequestProcessor reqProc = new RequestProcessorImpl(metrics, timestampOracle,
                persistProc, config.getMaxItems());

        // Setup netty listener
        ChannelFactory factory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setNameFormat("boss-%d").build()),
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setNameFormat("worker-%d").build()),
                (Runtime.getRuntime().availableProcessors() * 2 + 1) * 2);

        // Create the global ChannelGroup
        ChannelGroup channelGroup = new DefaultChannelGroup(TSOServer.class.getName());

        final TSOHandler handler = new TSOHandler(channelGroup, reqProc);

        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        bootstrap.setPipelineFactory(new TSOPipelineFactory(handler));

        // Add the parent channel to the group
        LOG.info("TSO service binding to port {}", config.getPort());
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

        /** Start the Netty shutdown */
        LOG.info("Close of channel group");
        channelGroup.close().awaitUninterruptibly();

        LOG.info("Release resources");
        factory.releaseExternalResources();
    }

    public void stop() {
        finish = true;
        synchronized (lock) {
            lock.notifyAll();
        }
    }
}
