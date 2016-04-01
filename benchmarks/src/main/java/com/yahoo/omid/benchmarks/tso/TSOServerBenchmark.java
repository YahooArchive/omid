/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.benchmarks.tso;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark using directly TSOClient to connect to the TSO Server
 */
public class TSOServerBenchmark implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TSOServerBenchmark.class);

    private volatile boolean isCleaningDone = false;

    private final TSOServerBenchmarkConfig expConfig;

    // Clients triggering txs (threads) & corresponding executor
    private final ArrayList<RawTxRunner> txRunners = new ArrayList<>();
    private final ScheduledExecutorService txRunnerExec;

    private TSOServerBenchmark(TSOServerBenchmarkConfig expConfig) throws IOException {

        this.expConfig = expConfig;

        // Executor for TxRunners (Clients triggering transactions)
        Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Thread {} threw exception", t, e);
            }
        };
        ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder()
                .setNameFormat("tx-runner-%d")
                .setUncaughtExceptionHandler(uncaughtExceptionHandler);
        this.txRunnerExec = Executors.newScheduledThreadPool(expConfig.getTxRunners(), threadFactoryBuilder.build());

    }

    public static void main(String[] args) throws Exception {

        final TSOServerBenchmarkConfig config = new TSOServerBenchmarkConfig();

        final int nOfTxRunners = config.getTxRunners();

        final int benchmarkTimeValue = config.getRunFor().getPeriod();
        final TimeUnit benchmarkTimeUnit = config.getRunFor().getTimeUnit();

        try (TSOServerBenchmark tsoBenchmark = new TSOServerBenchmark(config)) {

            tsoBenchmark.attachShutDownHook();

            LOG.info("----- Starting TSO Benchmark [ {} TxRunner clients ] -----", nOfTxRunners);

            for (int i = 0; i < nOfTxRunners; ++i) {
                tsoBenchmark.createTxRunner();
            }

            LOG.info("Benchmark run lenght {} {}", benchmarkTimeValue, benchmarkTimeUnit);
            benchmarkTimeUnit.sleep(benchmarkTimeValue);

        } finally {
            LOG.info("----- TSO Benchmark complete - Check metrics from individual clients in log -----");
        }

    }

    private void attachShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread("benchmark-cleaner") {
            @Override
            public void run() {
                if (!isCleaningDone) {
                    close();
                }
            }
        });
        LOG.info("Shutdown Hook Attached");
    }

    private void createTxRunner() throws IOException, InterruptedException, ExecutionException {

        RawTxRunner txRunner = new RawTxRunner(expConfig);
        txRunnerExec.submit(txRunner);

        txRunners.add(txRunner);

    }

    @Override
    public void close() {

        // Stop clients
        for (RawTxRunner txRunner : txRunners) {
            txRunner.stop();
        }

        // Shutdown executor
        try {
            LOG.info("Closing TxRunner Executor in 10 secs");
            boolean wasSuccess = txRunnerExec.awaitTermination(10, TimeUnit.SECONDS);
            if (!wasSuccess) {
                txRunnerExec.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.info("Interrupted whilst shutting down TxRunner Executor!");
        } finally {
            LOG.info("TxRunner Executor stopped");
        }

        isCleaningDone = true;
    }

}
