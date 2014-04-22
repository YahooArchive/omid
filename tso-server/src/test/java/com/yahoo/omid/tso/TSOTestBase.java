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
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.InMemoryCommitTable;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.client.TSOClient;
import com.yahoo.omid.client.TSOFuture;
import com.yahoo.omid.tso.util.ClientHandler;
import com.yahoo.omid.tso.util.TransactionClient;

public class TSOTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TSOTestBase.class);

    //private static Thread bkthread;
    //private static Thread tsothread;
    private static ExecutorService bkExecutor;
    private static ExecutorService tsoExecutor;
   
    protected static TSOClient client;
    protected static TSOClient client2;

    private static ChannelGroup channelGroup;
    private static ChannelFactory channelFactory;

    private static TSOServer tso;
   

    final static public RowKey r1 = new RowKey(new byte[] { 0xd, 0xe, 0xa, 0xd }, new byte[] { 0xb, 0xe, 0xe, 0xf });
    final static public RowKey r2 = new RowKey(new byte[] { 0xb, 0xa, 0xa, 0xd }, new byte[] { 0xc, 0xa, 0xf, 0xe });

    public static void setupClient(CommitTable.Client commitTable) throws IOException {

        // *** Start the Netty configuration ***
        Configuration conf = new BaseConfiguration();
        conf.setProperty("tso.host", "localhost");
        conf.setProperty("tso.port", 1234);

        // Create the associated Handler
        client = TSOClient.newBuilder().withConfiguration(conf)
            .withCommitTableClient(commitTable).build();

        client2 = TSOClient.newBuilder().withConfiguration(conf)
            .withCommitTableClient(commitTable).build();

    }
   
    public static void teardownClient() {
        // FIXME add client cleanup
    }

    @Before
    public void setupTSO() throws Exception {
        LOG.info("Starting TSO");
        CommitTable commitTable = new InMemoryCommitTable();
        tso = new TSOServer(TSOServerConfig.configFactory(1234, 0, recoveryEnabled(),
                                    4, 2, new String("localhost:2181"), 1000, 1000),
                            commitTable);

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
      
        // tso.stop();
        // if (tsoExecutor != null) {
        //     tsoExecutor.shutdownNow();
        // }
        // tso = null;
        // teardownClient();

        // TestUtils.waitForSocketNotListening("localhost", 1234, 1000);
      
        Thread.sleep(10);
    }

    protected boolean recoveryEnabled() {
        return false;
    }

}
