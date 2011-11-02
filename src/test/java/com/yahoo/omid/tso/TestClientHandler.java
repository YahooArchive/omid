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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.OperationNotSupportedException;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import com.yahoo.omid.tso.Committed;
import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.FullAbortReport;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.messages.TimestampResponse;
import com.yahoo.omid.tso.serialization.TSODecoder;
import com.yahoo.omid.tso.serialization.TSOEncoder;

/**
 * Example of ChannelHandler for the Transaction Client
 * 
 * @author maysam
 * 
 */
public class TestClientHandler extends SimpleChannelHandler {

   private static final Logger logger = Logger.getLogger(TestClientHandler.class.getName());

   /**
    * Return value for the caller
    */
   final BlockingQueue<Boolean> answer = new LinkedBlockingQueue<Boolean>();
   final BlockingQueue<TSOMessage> messageQueue = new LinkedBlockingQueue<TSOMessage>();

   private Committed committed = new Committed();
   private Set<Long> aborted = Collections.synchronizedSet(new HashSet<Long>(100000));

   private long largestDeletedTimestamp = 0;
   private long connectionTimestamp = Long.MAX_VALUE;
   
   private Channel channel;

   // Sends FullAbortReport upon receiving a CommitResponse with committed =
   // false
   private boolean autoFullAbort = true;

   /**
    * Method to wait for the final response
    * 
    * @return success or not
    */
   public boolean waitForAll() {
      for (;;) {
         try {
            return answer.take();
         } catch (InterruptedException e) {
            // Ignore.
         }
      }
   }

   /**
    * Constructor
    * 
    * @param nbMessage
    * @param inflight
    */
   public TestClientHandler() {
   }

   public void sendMessage(Object msg) {
      channel.write(msg);
   }

   public void clearMessages() {
      messageQueue.clear();
   }
   
   public void await() {
      synchronized(this) {
         while (channel == null) {
            try {
               wait();
            } catch (InterruptedException e) {
               return;
            }
         }
      }
   }

   public void receiveBootstrap() {
      receiveMessage(CommittedTransactionReport.class);
      receiveMessage(AbortedTransactionReport.class);
      receiveMessage(FullAbortReport.class);
      receiveMessage(LargestDeletedTimestampReport.class);
   }

   public Object receiveMessage() {
      try {
         Object msg = messageQueue.poll(5, TimeUnit.SECONDS);
         assertNotNull("Reception of message timed out", msg);
         return msg;
      } catch (InterruptedException e) {
         return null;
      }
   }

   @SuppressWarnings("unchecked")
   public <T extends TSOMessage> T receiveMessage(Class<T> type) {
      try {
         TSOMessage msg = messageQueue.poll(5, TimeUnit.SECONDS);
         assertNotNull("Reception of message timed out", msg);
         assertThat(msg, is(type));
         return (T) msg;
      } catch (InterruptedException e) {
         return null;
      }
   }

   /**
    * Add the ObjectXxcoder to the Pipeline
    */
   @Override
   public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
      e.getChannel().getPipeline().addFirst("decoder", new TSODecoder());
      e.getChannel().getPipeline().addAfter("decoder", "encoder", new TSOEncoder());
   }

   /**
    * Starts the traffic
    */
   @Override
   public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
      logger.log(Level.INFO, "Start sending traffic");
      synchronized (this) {
         this.channel = ctx.getChannel();
         notify();
      }

   }

   /**
    * When the channel is closed, print result
    */
   @Override
   public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
   }

   /**
    * When a message is received, handle it based on its type
    */
   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      Object msg = e.getMessage();
      if (msg instanceof CommitResponse) {
         handle((CommitResponse) msg, ctx.getChannel());
      } else if (msg instanceof TimestampResponse) {
         handle((TimestampResponse) msg, ctx.getChannel());
      } else if (msg instanceof AbortedTransactionReport) {
         handle((AbortedTransactionReport) msg, ctx.getChannel());
      } else if (msg instanceof CommittedTransactionReport) {
         handle((CommittedTransactionReport) msg, ctx.getChannel());
      } else if (msg instanceof FullAbortReport) {
         handle((FullAbortReport) msg, ctx.getChannel());
      }
      messageQueue.offer((TSOMessage) msg);
   }

   public void handle(TimestampResponse msg, Channel channel) {
      if (msg.timestamp < connectionTimestamp) {
         connectionTimestamp = msg.timestamp;
      }
   }

   public void handle(FullAbortReport msg, Channel channel) {
      aborted.remove(msg.startTimestamp);
   }

   public void handle(AbortedTransactionReport msg, Channel channel) {
      aborted.add(msg.startTimestamp);
   }

   public void handle(CommittedTransactionReport msg, Channel channel) {
      committed.commit(msg.startTimestamp, msg.commitTimestamp);
   }

   public void handle(CommitResponse msg, Channel channel) {
      if (!msg.committed && autoFullAbort) {// aborted
         FullAbortReport ack = new FullAbortReport(msg.startTimestamp);
         Channels.write(channel, ack);
      }
   }

   public void handle(LargestDeletedTimestampReport msg, Channel channel) {
      largestDeletedTimestamp = msg.largestDeletedTimestamp;
      committed.raiseLargestDeletedTransaction(msg.largestDeletedTimestamp);
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      if (e.getCause() instanceof IOException) {
         logger.log(Level.WARNING, "IOException from downstream.");
      } else {
         logger.log(Level.WARNING, "Unexpected exception from downstream.", e.getCause());
      }
      // Offer default object
      answer.offer(false);
      Channels.close(e.getChannel());
   }

   public boolean isAutoFullAbort() {
      return autoFullAbort;
   }

   public void setAutoFullAbort(boolean autoFullAbort) {
      this.autoFullAbort = autoFullAbort;
   }

   public boolean validRead(long transaction, long startTimestamp) {
      if (aborted.contains(transaction)) 
         return false;
      
      //
      // Tx B is half aborted due to the increase of largestDeletedTimestamp (Tx A knows this)
      // Tx A reads some values written by Tx B
      // Tx B cleans its values, notfies TSO and Tx A learns this
      // Tx A wonders if the values read before are valid
      //    Nor on aborted (Tx B has been fully aborted and Tx A learned it)  
      //    Nor committed
      //    Because Tx B < largestDeletedTimestamp, Tx A thinks its committed.
      // 
      //
      
      
      long commitTimestamp = committed.getCommit(transaction);
      if (commitTimestamp != -1)
         return commitTimestamp < startTimestamp;
      if (transaction > connectionTimestamp)
         return transaction <= largestDeletedTimestamp;
      return askTSO(transaction, startTimestamp); // Could be half aborted and we didnt get notified
   }

   private boolean askTSO(long transaction, long startTimestamp) {
      throw new RuntimeException(new OperationNotSupportedException("Must implement"));
   }
}
