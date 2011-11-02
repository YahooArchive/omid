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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CommitQueryRequest;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.FullAbortReport;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.TimestampResponse;
import com.yahoo.omid.tso.serialization.TSODecoder;
import com.yahoo.omid.tso.serialization.TSOEncoder;


/**
 * Example of ChannelHandler for the Transaction Client
 * @author maysam
 *
 */
@ChannelPipelineCoverage("one")
public class ClientHandler extends SimpleChannelHandler {

   private static final Log LOG = LogFactory.getLog(ClientHandler.class);

   /**
    * Maximum number of modified rows in each transaction
    */
   static final int MAX_ROW = 20;

   /**
    * The number of rows in database
    */
   static final int DB_SIZE = 20000000;

   /**
    * Maximum number if outstanding message
    */
   private final int MAX_IN_FLIGHT;

   /**
    * Number of message to do
    */
   private final int nbMessage;

   /**
    * Current rank (decreasing, 0 is the end of the game)
    */
   private int curMessage;

   /**
    * number of outstanding commit requests
    */
   //private final AtomicInteger outstandingTransactions = new AtomicInteger(0);
   private int outstandingTransactions = 0;

   /**
    * Start date
    */
   private Date startDate = null;

   /**
    * Stop date
    */
   private Date stopDate = null;

   /**
    * Return value for the caller
    */
   final BlockingQueue<Boolean> answer = new LinkedBlockingQueue<Boolean>();

   private Committed committed = new Committed();
   private Set<Long> aborted = Collections.synchronizedSet(new HashSet<Long>(100000));
   
   /*
    * For statistial purposes
    */
   public HashMap<Long, Long> wallClockTime = new HashMap<Long, Long>(); //from start timestmap to nonoTime

   private long largestDeletedTimestamp = 0;
   private long connectionTimestamp;
   private boolean hasConnectionTimestamp = false; 
   
   public long totalNanoTime = 0;
   public long totalTx = 0;
   private TSODecoder _decoder;

   /**
    * Method to wait for the final response
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
    * @param nbMessage
    * @param inflight
    */
   public ClientHandler(int nbMessage, int inflight, int localClients) {
      if (nbMessage < 0) {
         throw new IllegalArgumentException("nbMessage: " + nbMessage);
      }
      this.MAX_IN_FLIGHT = inflight;
      this.nbMessage = nbMessage;
      this.curMessage = nbMessage;
   }

   /**
    * Add the ObjectXxcoder to the Pipeline
    */
   @Override
      public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
         //e.getChannel().getPipeline().addLast("framer", new DelimiterBasedFrameDecoder(
                  //1500, Delimiters.nulDelimiter()));
       _decoder = new TSODecoder();
         e.getChannel().getPipeline().addFirst("decoder", _decoder);
         e.getChannel().getPipeline().addAfter("decoder", "encoder",
               new TSOEncoder());
      }

   /**
    * Starts the traffic
    */
   @Override
      public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
//         logger.log(Level.INFO, "Start sending traffic");
         startDate = new Date();
         startTransaction(e.getChannel());
      }

   /**
    * If write of Commit Request was not possible before, just do it now
    */
   @Override
      public void channelInterestChanged(ChannelHandlerContext ctx,
            ChannelStateEvent e) {
         startTransaction(e.getChannel());
      }

   /**
    * When the channel is closed, print result
    */
   @Override
      public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
      throws Exception {
      stopDate = new Date();
      String MB = String.format("Memory Used: %8.3f MB",
            (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
             .freeMemory()) / 1048576.0);
      String Mbs = String.format("%9.3f TPS",
            ((nbMessage - curMessage) * 1000 / (float) (stopDate
               .getTime() - (startDate!=null?startDate.getTime():0)) 
            ));
      System.out.println(MB + " " + Mbs);
      }

   /**
    * When a message is received, handle it based on its type
    */
   @Override
   public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      Object msg = e.getMessage();
      if (msg instanceof CommitResponse) {
         handle((CommitResponse) msg, ctx.getChannel());
         return;
      } else if (msg instanceof TimestampResponse) {
         handle((TimestampResponse) msg, ctx.getChannel());
         return;
      } else if (msg instanceof AbortedTransactionReport) {
         handle((AbortedTransactionReport) msg, ctx.getChannel());
         return;
      } else if (msg instanceof CommittedTransactionReport) {
         handle((CommittedTransactionReport) msg, ctx.getChannel());
         return;
      } else if (msg instanceof FullAbortReport) {
         handle((FullAbortReport) msg, ctx.getChannel());
         return;
      } else if (msg instanceof LargestDeletedTimestampReport) {
         handle((LargestDeletedTimestampReport) msg, ctx.getChannel());
         return;
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

   public void handle(LargestDeletedTimestampReport msg, Channel channel) {
//       System.out.println("Timestamps: " + msg.largestDeletedTimestamp + " " + _decoder.lastCommitTimestamp + " " + _decoder.lastStartTimestamp);
       largestDeletedTimestamp = msg.largestDeletedTimestamp;
       committed.raiseLargestDeletedTransaction(msg.largestDeletedTimestamp);
   }

   /**
    * Handle the TimestampResponse message
    */
   public void handle(TimestampResponse msg, Channel channel) {
      //System.out.println("Got: " + msg);
      if (!hasConnectionTimestamp) {
         hasConnectionTimestamp = true;
         connectionTimestamp = msg.timestamp;
      }
      sendCommitRequest(msg.timestamp, channel);
   }

   /**
    * Handle the CommitRequest message
    */
   private long lasttotalTx = 0;
   private long lasttotalNanoTime = 0;
   private long lastTimeout = System.currentTimeMillis();
   public void handle(CommitResponse msg, Channel channel) {
      //outstandingTransactions.decrementAndGet();
      outstandingTransactions--;
      long finishNanoTime = System.nanoTime();
      long startNanoTime = wallClockTime.remove(msg.startTimestamp);
      if (msg.committed) {
         totalNanoTime += (finishNanoTime - startNanoTime);
         totalTx ++;
         long timeout = System.currentTimeMillis();
//         if (totalTx % 10000 == 0) {//print out
         if (timeout - lastTimeout > 60*1000) { //print out
            long difftx = totalTx - lasttotalTx;
            long difftime = totalNanoTime - lasttotalNanoTime;
            System.out.format(" CLIENT: totalTx: %d totalNanoTime: %d microtime/tx: %4.3f tx/s %4.3f "
                  + "Size Com: %d Size Aborted: %d Diff: %d  Memory Used: %8.3f KB TPS:  %9.3f \n",
                  difftx, difftime, (difftime / (double) difftx / 1000), 1000 * difftx / ((double) (timeout - lastTimeout)),
                  getSizeCom(), getSizeAborted(), largestDeletedTimestamp - _decoder.lastStartTimestamp,
                  (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024.0,
                  ((nbMessage - curMessage) * 1000 / (float) (new Date().getTime() - (startDate != null ? startDate.getTime() : 0))));
            lasttotalTx = totalTx;
            lasttotalNanoTime = totalNanoTime;
            lastTimeout = timeout;
         }
      } else {//aborted
         FullAbortReport ack = new FullAbortReport(msg.startTimestamp);
         Channels.write(channel, ack);
      }
      startTransaction(channel);
   }
   
   private long getSizeCom() {
       return committed.getSize();
   }
   
   private long getSizeAborted() {
       return aborted.size() * 8 * 8;
   }

   @Override
      public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
         if (e.getCause() instanceof IOException) {
            LOG.warn("IOException from downstream.", e.getCause());
         } else {
            LOG.warn("Unexpected exception from downstream.",
                  e.getCause());
         }
         // Offer default object
         answer.offer(false);
         Channels.close(e.getChannel());
      }

   private java.util.Random rnd;

   /**
    * Sends the CommitRequest message to the channel
    * @param timestamp
    * @param channel
    */
   private void sendCommitRequest(final long timestamp, final Channel channel) {
      if ( !((channel.getInterestOps() & Channel.OP_WRITE) == 0) )
         return;

      final CommitRequest cr = new CommitRequest();

      //initialize rnd if it is not yet
      if (rnd == null) {
         long seed = System.currentTimeMillis();
         seed *= channel.getId();//to make it channel dependent
         rnd = new java.util.Random(seed);
      }

      byte size = (byte)rnd.nextInt(MAX_ROW);
      cr.rows = new RowKey[size];
      for (byte i = 0; i < cr.rows.length; i++) {
         //long l = rnd.nextLong();
         long l = rnd.nextInt(DB_SIZE);
         byte [] b = new byte[8];
         for(int iii= 0; iii < 8; iii++){
            b[7 - iii] = (byte)(l >>> (iii * 8));
         }
         byte[] tableId = new byte[8];
         cr.rows[i] = new RowKey(b, tableId);
      }

      cr.startTimestamp = timestamp;

      //send a query once in a while
      totalCommitRequestSent++;
      if (totalCommitRequestSent % QUERY_RATE == 0 && cr.rows.length > 0) {
         long queryTimeStamp = rnd.nextInt((int)timestamp);
         CommitQueryRequest query = new CommitQueryRequest(timestamp, queryTimeStamp);
         Channels.write(channel, query);
      }
      
      //keep statistics
      wallClockTime.put(timestamp, System.nanoTime());
      Channels.write(channel, cr);
   }

   private long totalCommitRequestSent;//just to keep the total number of commitreqeusts sent
   private int QUERY_RATE = 100;//send a query after this number of commit requests


   /**
    * Start a new transaction
    * @param channel
    */
   private void startTransaction(Channel channel) {
      while (true) {//fill the pipe with as much as request you can
         if ( !((channel.getInterestOps() & Channel.OP_WRITE) == 0) )
            return;

         //if (outstandingTransactions.intValue() >= MAX_IN_FLIGHT)
         if (outstandingTransactions >= MAX_IN_FLIGHT)
            return;

         if (curMessage == 0) {
            LOG.warn("No more message");
            //wait for all outstanding msgs and then close the channel
            //if (outstandingTransactions.intValue() == 0) {
            if (outstandingTransactions == 0) {
               LOG.warn("Close channel");
               channel.close().addListener(new ChannelFutureListener() {
                  public void operationComplete(ChannelFuture future) {
                     answer.offer(true);
                  }
               });
            }
            return;
         }
         curMessage --;
         TimestampRequest tr = new TimestampRequest();
         outstandingTransactions++;
         //outstandingTransactions.incrementAndGet();
         Channels.write(channel, tr);
         
         Thread.yield();
      }
   }
   
   public boolean validRead(long transaction, long startTimestamp) {
      if (aborted.contains(transaction)) 
         return false;
      long commitTimestamp = committed.getCommit(transaction);
      if (commitTimestamp != -1)
         return commitTimestamp < startTimestamp;
      if (hasConnectionTimestamp && transaction > connectionTimestamp)
         return transaction <= largestDeletedTimestamp;
      return askTSO(transaction, startTimestamp); // Could be half aborted and we didnt get notified
   }

   private boolean askTSO(long transaction, long startTimestamp) {
      throw (new UnsupportedOperationException("Must implement"));
   }
}
