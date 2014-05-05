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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.BaseConfiguration;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.InMemoryCommitTable;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.client.TSOClient;
import com.yahoo.omid.tso.util.DummyCellIdImpl;

public class TSOTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TSOTestBase.class);

    //private static Thread bkthread;
    //private static Thread tsothread;
    private static ExecutorService bkExecutor;
    private static ExecutorService tsoExecutor;

    protected static Configuration clientConf = new BaseConfiguration();
    protected static TSOClient client;
    protected static TSOClient client2;

    private static ChannelGroup channelGroup;
    private static ChannelFactory channelFactory;

    private static TSOServer tso;
    private static CommitTable commitTable = new InMemoryCommitTable();


    final static public CellId c1 = new DummyCellIdImpl(0xdeadbeefL);
    final static public CellId c2 = new DummyCellIdImpl(0xfeedcafeL);

    public static void setupClient(CommitTable.Client commitTable) throws IOException {

        clientConf.setProperty("tso.host", "localhost");
        clientConf.setProperty("tso.port", 1234);

        // Create the associated Handler
        client = TSOClient.newBuilder().withConfiguration(clientConf)
            .withCommitTableClient(commitTable).build();

        client2 = TSOClient.newBuilder().withConfiguration(clientConf)
            .withCommitTableClient(commitTable).build();

    }

    public TSOClient getClient() {
        return client;
    }

    public Configuration getClientConfiguration() {
        return clientConf;
    }

    public CommitTable getCommitTable() {
        return commitTable;
    }

    public static void teardownClient() {
        // FIXME add client cleanup
    }

    @Before
    public void setupTSO() throws Exception {
        LOG.info("Starting TSO");
        tso = new TSOServer(TSOServerConfig.configFactory(1234, 1000),
                            commitTable, new TimestampOracle.InMemoryTimestampStorage());

        tsoExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("tsomain-%d").build());
        tsoExecutor.execute(tso);
        TestUtils.waitForSocketListening("localhost", 1234, 100);
        LOG.info("Finished loading TSO");

        Thread.currentThread().setName("JUnit Thread");

        setupClient(commitTable.getClient().get());
    }

    @After
    public void teardownTSO() throws Exception {

        // IKFIXME      clientHandler.sendMessage(new TimestampRequest());
        // while (!(clientHandler.receiveMessage() instanceof TimestampResponse))
        //    ; // Do nothing
        // clientHandler.clearMessages();
        // clientHandler.setAutoFullAbort(true);
        // secondClientHandler.sendMessage(new TimestampRequest());
        // while (!(secondClientHandler.receiveMessage() instanceof TimestampResponse))
        //    ; // Do nothing
        // secondClientHandler.clearMessages();
        // secondClientHandler.setAutoFullAbort(true);

        tso.stop();
        if (tsoExecutor != null) {
            tsoExecutor.shutdownNow();
        }
        tso = null;

        TestUtils.waitForSocketNotListening("localhost", 1234, 1000);

    }

    protected boolean recoveryEnabled() {
        return false;
    }

}