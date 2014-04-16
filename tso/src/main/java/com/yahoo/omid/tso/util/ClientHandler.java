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

package com.yahoo.omid.tso.util;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yahoo.omid.client.TSOClient;
import com.yahoo.omid.client.TSOFuture;

import com.yahoo.omid.tso.Committed;
import com.yahoo.omid.tso.RowKey;
import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.TimestampResponse;

/**
 * Example of ChannelHandler for the Transaction Client
 * 
 * @author maysam
 * 
 */
public class ClientHandler {
    
    public enum RowDistribution {
        
        UNIFORM,ZIPFIAN
    }

    static int threadCounter = 0;
    private int threadId = threadCounter++;

    private IntegerGenerator intGenerator;
   
    private CountDownLatch signalInitialized = new CountDownLatch(1);

    private static final Logger LOG = LoggerFactory.getLogger(ClientHandler.class);


    /**
     * Maximum number of modified rows in each transaction
     */
    final int maxTxSize;
    public static final int DEFAULT_MAX_ROW = 20;

    /**
     * The number of rows in database
     */
    public static final int DEFAULT_DB_SIZE = 20000000;

    public static final long PAUSE_LENGTH = 50; // in ms

    /**
     * Maximum number if outstanding messages
     */
    private final int maxInFlight;

    /**
     * Number of message to do
     */
    private final long nbMessage;

    /**
     * Number of second to run for
     */
    private volatile boolean timedOut = false;

    /**
     * Current rank (decreasing, 0 is the end of the game)
     */
    private long curMessage;

    /**
     * number of outstanding commit requests
     */
    private int outstandingTransactions = 0;


    private long getSizeCom() {
        return committed.getSize();
    }

    private long getSizeAborted() {
        return aborted.size() * 8 * 8;
    }

    private java.util.Random rnd;
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
    public ConcurrentHashMap<Long, Long> wallClockTime = new ConcurrentHashMap<Long, Long>(); 

    public long totalNanoTime = 0;
    public long totalTx = 0;
   
    private Channel channel;

    private float percentReads;

    final Semaphore outstanding;
    final TSOClient client;

    /**
     * Constructor
     * 
     * @param nbMessage
     * @param inflight
     * @throws IOException
     */
    public ClientHandler(Configuration conf, int runFor, long nbMessage, int inflight, boolean pauseClient, 
                         float percentReads, int maxTxSize, IntegerGenerator intGenerator) throws IOException {
        if (nbMessage < 0) {
            throw new IllegalArgumentException("nbMessage: " + nbMessage);
        }
        this.maxInFlight = inflight;
        this.nbMessage = nbMessage;
        this.curMessage = nbMessage;
        this.pauseClient = pauseClient;
        this.percentReads = percentReads;
        this.maxTxSize = maxTxSize;
        this.intGenerator = intGenerator;
        this.signalInitialized.countDown();

        this.outstanding = new Semaphore(this.maxInFlight);

        client = new TSOClient(conf);

        long seed = System.currentTimeMillis();
        seed *= channel.getId();// to make it channel dependent
        rnd = new java.util.Random(seed);

        executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            outstanding.acquire();
                            final TSOFuture<Long> f = client.createTransaction();
                            f.addListener(new TimestampListener(f), executor);
                        }
                    } catch (InterruptedException ie) {
                        // normal, ignore, we're shutting down
                    }
                }
            });
    }

    class TimestampListener implements Runnable {
        TSOFuture<Long> f;

        TimestampListener(TSOFuture<Long> f) {
            
        }

        @Override
        public void run() {
            ;
        }
    }

    class DeferredListener implements Runnable {
        final long txnid;

        DeferredListener(long txnid) {
            this.txnid = txnid;
        }

        @Override
        public void run() {
            boolean readOnly = (rnd.nextFloat() * 100) < percentReads;

            int size = readOnly ? 0 : rnd.nextInt(maxTxSize);
            final RowKey [] rows = new RowKey[size];
            for (byte i = 0; i < rows.length; i++) {
                long l = intGenerator.nextInt();
                byte[] b = new byte[8];
                for (int iii = 0; iii < 8; iii++) {
                    b[7 - iii] = (byte) (l >>> (iii * 8));
                }
                byte[] tableId = new byte[8];
                rows[i] = new RowKey(b, tableId);
            }
            final TSOFuture<Long> f = client.commit(txnid, rows);
            f.addListener(new CommitListener(f), executor);
        }
    }

    class CommitListener implements Runnable {
        TSOFuture<Long> f;

        CommitListener(TSOFuture<Long> f) {
            this.f = f;
        }

        @Override
        public void run() {
            outstanding.release();
        }
    }
            
    void shutdown() {
    }



    private boolean pauseClient;

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(2,
            new ThreadFactoryBuilder().setNameFormat("client-thread-" + threadId + "-%d").build());

    private long totalCommitRequestSent;// just to keep the total number of
    // commitreqeusts sent
    private int QUERY_RATE = 100;// send a query after this number of commit
    // requests


}
