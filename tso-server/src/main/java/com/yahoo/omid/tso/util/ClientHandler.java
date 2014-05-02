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

import static com.codahale.metrics.MetricRegistry.name;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.client.TSOClient;
import com.yahoo.omid.client.TSOFuture;
import com.yahoo.omid.tso.CellId;

/**
 * Example of ChannelHandler for the Transaction Client
 * 
 * @author maysam
 * 
 */
public class ClientHandler {

    public enum RowDistribution {

        UNIFORM, ZIPFIAN
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
    final int commitDelay;

    Timer timestampTimer;
    Timer commitTimer;
    Timer abortTimer;
    Counter errorCounter;

    /**
     * Constructor
     * 
     * @param nbMessage
     * @param inflight
     * @throws IOException
     */
    public ClientHandler(Configuration conf, MetricRegistry metrics, int runFor, long nbMessage, int inflight,
            int commitDelay, float percentReads, int maxTxSize, IntegerGenerator intGenerator) throws IOException {
        if (nbMessage < 0) {
            throw new IllegalArgumentException("nbMessage: " + nbMessage);
        }

        this.maxInFlight = inflight;
        this.nbMessage = nbMessage;
        this.curMessage = nbMessage;
        this.commitDelay = commitDelay;
        this.percentReads = percentReads;
        this.maxTxSize = maxTxSize;
        this.intGenerator = intGenerator;
        this.signalInitialized.countDown();
        this.outstanding = new Semaphore(this.maxInFlight);

        String name = "thread-" + threadId;
        timestampTimer = metrics.timer(name("TSOClient", name, "timestamp"));
        commitTimer = metrics.timer(name("TSOClient", name, "commit"));
        abortTimer = metrics.timer(name("TSOClient", name, "abort"));
        errorCounter = metrics.counter(name("TSOClient", name, "errors"));

        client = TSOClient.newBuilder().withConfiguration(conf).withMetrics(metrics).build();

        long seed = System.currentTimeMillis();
        seed *= threadId;// to make it channel dependent
        rnd = new java.util.Random(seed);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        outstanding.acquire();
                        long start = System.nanoTime();
                        final TSOFuture<Long> f = client.createTransaction();
                        f.addListener(new TimestampListener(f, start), executor);
                    }
                } catch (InterruptedException ie) {
                    // normal, ignore, we're shutting down
                }
            }
        });
    }

    class TimestampListener implements Runnable {
        TSOFuture<Long> f;
        final long start;

        TimestampListener(TSOFuture<Long> f, long start) {
            this.f = f;
            this.start = start;
        }

        @Override
        public void run() {
            try {
                long timestamp = f.get();
                timestampTimer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                executor.schedule(new DeferredListener(timestamp), commitDelay, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                errorCounter.inc();
            }
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
            final Set<CellId> cells = new HashSet<CellId>();
            for (byte i = 0; i < size; i++) {
                long l = intGenerator.nextInt();                
                cells.add(new DummyCellIdImpl(l));
            }
            final TSOFuture<Long> f = client.commit(txnid, cells);
            f.addListener(new CommitListener(f, System.nanoTime()), executor);
        }
    }

    class CommitListener implements Runnable {
        TSOFuture<Long> f;
        final long start;

        CommitListener(TSOFuture<Long> f, long start) {
            this.f = f;
            this.start = start;
        }

        @Override
        public void run() {
            try {
                f.get();
                commitTimer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            } catch (ExecutionException e) {
                abortTimer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            } catch (Exception e) {
                errorCounter.inc();
            }
            outstanding.release();
        }
    }

    void shutdown() {
    }

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(
            2,
            new ThreadFactoryBuilder().setNameFormat("client-thread-" + threadId + "-%d")
                    .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                            LOG.error("Thread {} threw exception", t, e);
                        }
                    }).build());

    private long totalCommitRequestSent;// just to keep the total number of
    // commitreqeusts sent
    private int QUERY_RATE = 100;// send a query after this number of commit
    // requests

}

