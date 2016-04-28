/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.benchmarks.tso;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.omid.benchmarks.utils.IntegerGenerator;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.Counter;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.Timer;
import org.apache.omid.tso.util.DummyCellIdImpl;
import org.apache.omid.tso.client.AbortException;
import org.apache.omid.tso.client.CellId;
import org.apache.omid.tso.client.OmidClientConfiguration;
import org.apache.omid.tso.client.TSOClient;
import org.apache.omid.tso.client.TSOFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

class RawTxRunner implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(RawTxRunner.class);

    private static volatile int txRunnerCounter = 0;
    private int txRunnerId = txRunnerCounter++;

    // Config params
    private final int writesetSize;
    private final boolean fixedWriteSetSize;
    private final long commitDelayInMs;
    private final int percentageOfReadOnlyTxs;
    private final IntegerGenerator cellIdGenerator;
    private final Random randomGen;

    // Main elements
    private final TSOClient tsoClient;
    private final CommitTable.Client commitTableClient;

    // Asynchronous executor for tx post begin sequence: TimestampListener -> Committer -> CommitListener
    private final ScheduledExecutorService callbackExec =
            Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
                    .setNameFormat("tx-runner-" + txRunnerId + "-callback")
                    .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                            LOG.error("Thread {} threw exception", t, e);
                        }
                    }).build());

    // Statistics to save
    private final Timer timestampTimer;
    private final Timer commitTimer;
    private final Timer abortTimer;
    private final Counter errorCounter;

    // Allows to setup a maximum rate for the client in req/sec
    private final RateLimiter rateLimiter;

    // Is this TxRunner still running?
    private volatile boolean isRunning = false;

    RawTxRunner(final TSOServerBenchmarkConfig expConfig) throws IOException, InterruptedException {

        // Injector configuration
        List<Module> guiceModules = new ArrayList<>();
        guiceModules.add(new Module() {
            @Override
            public void configure(Binder binder) {
                binder.bind(MetricsRegistry.class).toInstance(expConfig.getMetrics());
            }
        });
        guiceModules.add(expConfig.getCommitTableStoreModule());
        Injector injector = Guice.createInjector(guiceModules);

        // Tx Runner config
        this.writesetSize = expConfig.getWritesetSize();
        this.fixedWriteSetSize = expConfig.isFixedWritesetSize();
        this.commitDelayInMs = expConfig.getCommitDelayInMs();
        this.percentageOfReadOnlyTxs = expConfig.getPercentageOfReadOnlyTxs();
        this.cellIdGenerator = expConfig.getCellIdGenerator();
        this.randomGen = new Random(System.currentTimeMillis() * txRunnerId); // to make it channel dependent

        int txRateInReqPerSec = expConfig.getTxRateInRequestPerSecond();
        long warmUpPeriodInSecs = expConfig.getWarmUpPeriodInSecs();

        LOG.info("TxRunner-{} [ Tx Rate (Req per Sec) -> {} ]", txRunnerId, txRateInReqPerSec);
        LOG.info("TxRunner-{} [ Warm Up Period -> {} Secs ]", txRunnerId, warmUpPeriodInSecs);
        LOG.info("TxRunner-{} [ Cell Id Distribution Generator -> {} ]", txRunnerId, expConfig.getCellIdGenerator().getClass());
        LOG.info("TxRunner-{} [ Max Tx Size -> {} Fixed: {} ]", txRunnerId, writesetSize, fixedWriteSetSize);
        LOG.info("TxRunner-{} [ Commit delay -> {} Ms ]", txRunnerId, commitDelayInMs);
        LOG.info("TxRunner-{} [ % of Read-Only Tx -> {} % ]", txRunnerId, percentageOfReadOnlyTxs);

        // Commit table client initialization
        CommitTable commitTable = injector.getInstance(CommitTable.class);
        this.commitTableClient = commitTable.getClient();

        // Stat initialization
        MetricsRegistry metrics = injector.getInstance(MetricsRegistry.class);
        String hostName = InetAddress.getLocalHost().getHostName();
        this.timestampTimer = metrics.timer(name("tx_runner", Integer.toString(txRunnerId), hostName, "timestamp"));
        this.commitTimer = metrics.timer(name("tx_runner", Integer.toString(txRunnerId), hostName, "commit"));
        this.abortTimer = metrics.timer(name("tx_runner", Integer.toString(txRunnerId), hostName, "abort"));
        this.errorCounter = metrics.counter(name("tx_runner", Integer.toString(txRunnerId), hostName, "errors"));
        LOG.info("TxRunner-{} [ Metrics provider module -> {} ]", txRunnerId, expConfig.getMetrics().getClass());

        // TSO Client initialization
        OmidClientConfiguration tsoClientConf = expConfig.getOmidClientConfiguration();
        this.tsoClient = TSOClient.newInstance(tsoClientConf);
        LOG.info("TxRunner-{} [ Connection Type {}/Connection String {} ]", txRunnerId,
                 tsoClientConf.getConnectionType(), tsoClientConf.getConnectionString());

        // Limiter for configured request per second
        this.rateLimiter = RateLimiter.create((double) txRateInReqPerSec, warmUpPeriodInSecs, TimeUnit.SECONDS);
    }

    @Override
    public void run() {

        isRunning = true;

        while (isRunning) {
            rateLimiter.acquire();
            long tsRequestTime = System.nanoTime();
            final TSOFuture<Long> tsFuture = tsoClient.getNewStartTimestamp();
            tsFuture.addListener(new TimestampListener(tsFuture, tsRequestTime), callbackExec);
        }

        shutdown();

    }

    void stop() {
        isRunning = false;
    }

    private void shutdown() {

        try {
            LOG.info("Finishing TxRunner in 3 secs", txRunnerId);
            boolean wasSuccess = callbackExec.awaitTermination(3, TimeUnit.SECONDS);
            if (!wasSuccess) {
                callbackExec.shutdownNow();
            }
            commitTableClient.close();
            tsoClient.close().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // ignore
        } catch (ExecutionException | IOException e) {
            // ignore
        } finally {
            LOG.info("TxRunner {} finished", txRunnerId);
        }

    }

    private class TimestampListener implements Runnable {

        final TSOFuture<Long> tsFuture;
        final long tsRequestTime;

        TimestampListener(TSOFuture<Long> tsFuture, long tsRequestTime) {
            this.tsFuture = tsFuture;
            this.tsRequestTime = tsRequestTime;
        }

        @Override
        public void run() {

            try {
                long txId = tsFuture.get();
                timestampTimer.update(System.nanoTime() - tsRequestTime);
                if (commitDelayInMs <= 0) {
                    callbackExec.execute(new Committer(txId));
                } else {
                    callbackExec.schedule(new Committer(txId), commitDelayInMs, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                errorCounter.inc();
            } catch (ExecutionException e) {
                errorCounter.inc();
            }

        }

    }

    private class Committer implements Runnable {

        final long txId;

        Committer(long txId) {
            this.txId = txId;
        }

        @Override
        public void run() {

            int txWritesetSize = calculateTxWritesetSize();

            if (txWritesetSize == 0) {
                return; // Read only tx, no need to commit
            }
            // Otherwise, we create the writeset...
            final Set<CellId> cells = new HashSet<>();
            for (byte i = 0; i < txWritesetSize; i++) {
                long cellId = cellIdGenerator.nextInt();
                cells.add(new DummyCellIdImpl(cellId));
            }
            // ... and we commit the transaction
            long startCommitTimeInNs = System.nanoTime();
            final TSOFuture<Long> commitFuture = tsoClient.commit(txId, cells);
            commitFuture.addListener(new CommitListener(txId, commitFuture, startCommitTimeInNs), callbackExec);

        }

        private int calculateTxWritesetSize() {
            int txSize = 0;
            boolean readOnly = (randomGen.nextFloat() * 100) < percentageOfReadOnlyTxs;
            if (!readOnly) {
                if (fixedWriteSetSize) {
                    txSize = writesetSize;
                } else {
                    txSize = randomGen.nextInt(writesetSize) + 1;
                }
            }
            return txSize;
        }

    }

    private class CommitListener implements Runnable {

        final long txId;
        final long commitRequestTime;
        final TSOFuture<Long> commitFuture;

        CommitListener(long txId, TSOFuture<Long> commitFuture, long commitRequestTime) {
            this.txId = txId;
            this.commitFuture = commitFuture;
            this.commitRequestTime = commitRequestTime;
        }

        @Override
        public void run() {

            try {
                commitFuture.get();
                commitTableClient.completeTransaction(txId).get();
                commitTimer.update(System.nanoTime() - commitRequestTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                errorCounter.inc();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof AbortException) {
                    abortTimer.update(System.nanoTime() - commitRequestTime);
                } else {
                    errorCounter.inc();
                }
            }

        }

    }

}
