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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.yahoo.omid.tso.ClientHandler;
import com.yahoo.omid.tso.RowKey;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOState;
import com.yahoo.omid.tso.TransactionClient;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.TimestampResponse;

public class TSOTestBase {
   private static final Log LOG = LogFactory.getLog(TSOTestBase.class);

   private static Thread bkthread;
   private static Thread tsothread;

   protected static TestClientHandler clientHandler;
   protected static TestClientHandler secondClientHandler;
   private static ChannelGroup channelGroup;
   private static ChannelFactory channelFactory;
   protected static TSOState state;
   private static TSOServer tso;
   

   final static public RowKey r1 = new RowKey(new byte[] { 0xd, 0xe, 0xa, 0xd }, new byte[] { 0xb, 0xe, 0xe, 0xf });
   final static public RowKey r2 = new RowKey(new byte[] { 0xb, 0xa, 0xa, 0xd }, new byte[] { 0xc, 0xa, 0xf, 0xe });

   public static void setupClient() {

      // *** Start the Netty configuration ***

      // Start client with Nb of active threads = 3 as maximum.
      channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(), 3);
      // Create the bootstrap
      ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
      // Create the global ChannelGroup
      channelGroup = new DefaultChannelGroup(TransactionClient.class.getName());
      // Create the associated Handler
      clientHandler = new TestClientHandler();

      // Add the handler to the pipeline and set some options
      bootstrap.getPipeline().addLast("handler", clientHandler);
      bootstrap.setOption("tcpNoDelay", false);
      bootstrap.setOption("keepAlive", true);
      bootstrap.setOption("reuseAddress", true);
      bootstrap.setOption("connectTimeoutMillis", 100);

      // *** Start the Netty running ***

      System.out.println("PARAM MAX_ROW: " + ClientHandler.MAX_ROW);
      System.out.println("PARAM DB_SIZE: " + ClientHandler.DB_SIZE);

      // Connect to the server, wait for the connection and get back the channel
      Channel channel = bootstrap.connect(new InetSocketAddress("localhost", 1234)).awaitUninterruptibly().getChannel();
      clientHandler.await();
      
      // Add the parent channel to the group
      channelGroup.add(channel);
      
      // Second client handler
      secondClientHandler = new TestClientHandler();
      bootstrap = new ClientBootstrap(channelFactory);
   // Add the handler to the pipeline and set some options
      bootstrap.getPipeline().addLast("handler", secondClientHandler);
      bootstrap.setOption("tcpNoDelay", false);
      bootstrap.setOption("keepAlive", true);
      bootstrap.setOption("reuseAddress", true);
      bootstrap.setOption("connectTimeoutMillis", 100);

      // *** Start the Netty running ***

      System.out.println("PARAM MAX_ROW: " + ClientHandler.MAX_ROW);
      System.out.println("PARAM DB_SIZE: " + ClientHandler.DB_SIZE);

      // Connect to the server, wait for the connection and get back the channel
      channel = bootstrap.connect(new InetSocketAddress("localhost", 1234)).awaitUninterruptibly().getChannel();
      secondClientHandler.await();
      
      // Add the parent channel to the group
      channelGroup.add(channel);
   }
   
   public static void teardownClient() {
      // Now close all channels
      System.out.println("close channelGroup");
      channelGroup.close().awaitUninterruptibly();
      // Now release resources
      System.out.println("close external resources");
      channelFactory.releaseExternalResources();
   }

   @BeforeClass
   public static void setupBookkeeper() throws Exception {
      System.out.println("PATH : "
            + System.getProperty("java.library.path"));
      
      if (bkthread == null) { 
         bkthread = new Thread() {
            public void run() {
               try {
                  Thread.currentThread().setName("BookKeeper");
                  String[] args = new String[1];
                  args[0] = "5";
                  LOG.info("Starting bk");
                  LocalBookKeeper.main(args);
               } catch (InterruptedException e) {
                  // go away quietly
               } catch (Exception e) {
                  LOG.error("Error starting local bk", e);
               }
            }
         };
   
         bkthread.start();
   
         if (!LocalBookKeeper.waitForServerUp("localhost:2181", 10000)) {
            throw new Exception("Error starting zookeeper/bookkeeper");
         }
      }
   }

   @AfterClass
   public static void teardownBookkeeper() throws Exception {

      if (bkthread != null) {
         bkthread.interrupt();
         bkthread.join();
      }
      teardownClient();
      waitForSocketNotListening("localhost", 1234);
      
      Thread.sleep(10);
   }
   
   @Before
   public void setupTSO() throws Exception {
      Thread.sleep(10);
      
      tso = new TSOServer(1234, 0, 4, 2, new String[] {"localhost:2181"});
      tsothread = new Thread(tso);
      
      LOG.info("Starting TSO");
      tsothread.start();
      waitForSocketListening("localhost", 1234);
      LOG.info("Finished loading TSO");
      
      state = tso.getState();
      
      Thread.currentThread().setName("JUnit Thread");
      
      setupClient();
   }
   
   @After
   public void teardownTSO() throws Exception {


      clientHandler.sendMessage(new TimestampRequest());
      while (!(clientHandler.receiveMessage() instanceof TimestampResponse))
         ; // Do nothing
      clientHandler.clearMessages();
      clientHandler.setAutoFullAbort(true);
      secondClientHandler.sendMessage(new TimestampRequest());
      while (!(secondClientHandler.receiveMessage() instanceof TimestampResponse))
         ; // Do nothing
      secondClientHandler.clearMessages();
      secondClientHandler.setAutoFullAbort(true);
      
      tso.stop();
      if (tsothread != null) {
         tsothread.interrupt();
         tsothread.join();
      }
      
      teardownClient();
      waitForSocketNotListening("localhost", 1234);
      
      Thread.sleep(10);
   }

   private static void waitForSocketListening(String host, int port) throws UnknownHostException, IOException,
         InterruptedException {
      while (true) {
         Socket sock = null;
         try {
            sock = new Socket(host, port);
         } catch (IOException e) {
            // ignore as this is expected
            Thread.sleep(100);
            continue;
         } finally {
            if (sock != null) {
               sock.close();
            }
         }
         LOG.info(host + ":" + port + " is UP");
         break;
      }
   }

   private static void waitForSocketNotListening(String host, int port) throws UnknownHostException, IOException,
         InterruptedException {
      while (true) {
         Socket sock = null;
         try {
            sock = new Socket(host, port);
         } catch (IOException e) {
            // ignore as this is expected
            break;
         } finally {
            if (sock != null) {
               sock.close();
            }
         }
         Thread.sleep(1000);
         LOG.info(host + ":" + port + " is still up");
      }
   }

}