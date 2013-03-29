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
package com.yahoo.omid.examples.notifications;

import static com.yahoo.omid.examples.Constants.COLUMN_1;
import static com.yahoo.omid.examples.Constants.COLUMN_FAMILY_1;
import static com.yahoo.omid.examples.Constants.TABLE_1;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.client.CommitUnsuccessfulException;
import com.yahoo.omid.client.TransactionException;
import com.yahoo.omid.client.TransactionManager;
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.client.TransactionalTable;
import com.yahoo.omid.examples.Constants;
import com.yahoo.omid.examples.notifications.ExamplesUtils.ExtendedPosixParser;
import com.yahoo.omid.notifications.TransactionCommittedRegionCoprocessor;
import com.yahoo.omid.notifications.metrics.MetricsUtils;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

/**
 * This applications shows the basic usage of the Omid's notification framework
 * 
 */
public class SimpleAppInjector {

    private static final Logger logger = Logger.getLogger(SimpleAppInjector.class);

    /**
     * Launches ObserverRegistrationService and perform an observer registration
     * 
     * @param args
     */
    @SuppressWarnings("static-access")
    public static void main(String[] args) throws Exception {

        CommandLineParser cmdLineParser = new ExtendedPosixParser(true);

        Options options = new Options();
        options.addOption("createDB", false, "If present, the database will be re-created");
        options.addOption(OptionBuilder.withLongOpt("injectors").withDescription("Number of tasks to inject txs")
                .withType(Number.class).hasArg().withArgName("argname").create());
        options.addOption(OptionBuilder.withLongOpt("txRate")
                .withDescription("Number of txs/s to execute per injector thread").withType(Number.class).hasArg()
                .withArgName("tx_rate").create());
        options.addOption(OptionBuilder.withArgName("hbase_master").hasArg()
                .withDescription("HBase Master server: host1:port1").create("hbase"));
        options.addOption(OptionBuilder.withArgName("omid_server").hasArg().withDescription("Omid server: host1:port1")
                .create("omid"));
        options.addOption(OptionBuilder.withArgName("metrics_output_dir").hasArg()
                .withDescription("Output directory for Metrics").create("metricsOutputDir"));

        boolean createDB = false;
        int nOfInjectorTasks = 1;
        int txRate = 1;
        String hbase = "localhost:2181";
        String omid = "localhost:1234";
        boolean writeMetricsToFile = false;
        String metricsOutputDir = System.getProperty("user.dir");

        try {
            CommandLine cmdLine = cmdLineParser.parse(options, args);

            if (cmdLine.hasOption("createDB")) {
                createDB = true;
            }

            if (cmdLine.hasOption("injectors")) {
                nOfInjectorTasks = ((Number) cmdLine.getParsedOptionValue("injectors")).intValue();
            }

            if (cmdLine.hasOption("txRate")) {
                txRate = ((Number) cmdLine.getParsedOptionValue("txRate")).intValue();
            }

            if (cmdLine.hasOption("hbase")) {
                hbase = cmdLine.getOptionValue("hbase");
            }

            if (cmdLine.hasOption("omid")) {
                omid = cmdLine.getOptionValue("omid");
            }

            if (cmdLine.hasOption("metricsOutputDir")) {
                writeMetricsToFile = true;
                metricsOutputDir = cmdLine.getOptionValue("metricsOutputDir");
                logger.info(metricsOutputDir);
            }
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // HBase table creation
        Configuration hbaseConf = HBaseConfiguration.create();
        final HostAndPort hbaseHp = HostAndPort.fromString(hbase);
        hbaseConf.set("hbase.zookeeper.quorum", hbaseHp.getHostText());
        hbaseConf.set("hbase.zookeeper.property.clientPort", Integer.toString(hbaseHp.getPortOrDefault(2181)));

        if (createDB) {
            createTables(hbaseConf);
            System.exit(0);
        }

        // TSO Client setup
        Configuration tsoClientConf = HBaseConfiguration.create();
        final HostAndPort omidHp = HostAndPort.fromString(omid);
        tsoClientConf.set("tso.host", omidHp.getHostText());
        tsoClientConf.setInt("tso.port", omidHp.getPortOrDefault(1234));

        final ExecutorService injectors = Executors.newFixedThreadPool(nOfInjectorTasks, new ThreadFactoryBuilder()
                .setNameFormat("Injector-%d").build());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                injectors.shutdown();
                logger.info("ooo Injectors ooo - Stopped (CTRL+C) - ooo Injectors ooo");
            }
        });

        // Metrics
        String metricsConfig;
        if (writeMetricsToFile) {
            metricsConfig = "csv:" + metricsOutputDir + ":10:SECONDS";
        } else {
            metricsConfig = "console:10:SECONDS";
        }
        MetricsUtils.initMetrics(metricsConfig);
        final Meter invocations = Metrics.newMeter(SimpleAppInjector.class, "injector@invocations", "invocations",
                TimeUnit.SECONDS);

        CountDownLatch startCdl = new CountDownLatch(1);
        for (int i = 0; i < nOfInjectorTasks; i++) {
            injectors.execute(new EventInjectorTask(tsoClientConf, startCdl, txRate, invocations));
        }
        logger.info("ooo Injectors ooo - STARTING " + nOfInjectorTasks + " LOOP TASKS INJECTING AT " + txRate
                + " TX/S IN COLUMN " + COLUMN_1 + " - ooo Injectors ooo");
        startCdl.countDown();
        logger.info("ooo Injectors ooo - OMID'S NOTIFICATION APP INJECTING LOAD TILL STOPPED - ooo Injectors ooo");

    }

    private static class EventInjectorTask implements Runnable {

        private CountDownLatch startCdl;
        private TransactionManager tm;
        private TransactionalTable tt;
        private int txRate;
        private Meter invocations;

        public EventInjectorTask(Configuration conf, CountDownLatch startCdl, int txRate, Meter invocations) {
            try {
                this.tm = new TransactionManager(conf);
                this.tt = new TransactionalTable(conf, TABLE_1);
            } catch (Exception e) {
                e.printStackTrace();
            }
            this.startCdl = startCdl;
            this.txRate = txRate;
            this.invocations = invocations;
        }

        public void run() {
            final Random randGen = new Random();
            RateLimiter rateLimiter = RateLimiter.create(txRate, 1, TimeUnit.MINUTES);
            try {
                startCdl.await();
                while (true) {
                    rateLimiter.acquire();
                    TransactionState tx = null;
                    try {
                        // Transaction adding to rows to a table
                        tx = tm.beginTransaction();
                        String rowId = Long.toString(randGen.nextLong());
                        doTransactionalPut(tx, tt, Bytes.toBytes("row-" + rowId), Bytes.toBytes(COLUMN_FAMILY_1),
                                Bytes.toBytes(COLUMN_1), Bytes.toBytes("injector wrote on row-" + rowId));
                        tm.tryCommit(tx);
                    } catch (CommitUnsuccessfulException e) {
                        logger.warn("Could not commit Tx " + tx, e);
                    } catch (IOException e) {
                        logger.warn("Error during transactional put ", e);
                    } catch (TransactionException e) {
                        logger.warn("Tx exception ", e);
                    } finally {
                        invocations.mark();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                try {
                    tt.close();
                } catch (IOException e) { /* Do nothing */
                }
            }
        }

        private void doTransactionalPut(TransactionState tx, TransactionalTable tt, byte[] rowName, byte[] colFamName,
                byte[] colName, byte[] dataValue) throws IOException {
            Put row = new Put(rowName);
            row.add(colFamName, colName, dataValue);
            tt.put(tx, row);
        }

    }

    private static void createTables(Configuration hbaseConf) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        logger.info("ooo DB ooo RE-CREATING TABLES ooo DB ooo");
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        if (admin.tableExists(TABLE_1)) {
            if (admin.isTableEnabled(TABLE_1)) {
                admin.disableTable(TABLE_1);
            }
            admin.deleteTable(Constants.TABLE_1);
        }
        HTableDescriptor table = new HTableDescriptor(TABLE_1);
        table.addCoprocessor(TransactionCommittedRegionCoprocessor.class.getName(), null, Coprocessor.PRIORITY_USER,
                null);
        HColumnDescriptor columnFamily = new HColumnDescriptor(COLUMN_FAMILY_1);
        columnFamily.setMaxVersions(Integer.MAX_VALUE);
        table.addFamily(columnFamily);

        admin.createTable(table);

        HTableDescriptor[] tables = admin.listTables();
        for (HTableDescriptor t : tables) {
            logger.info("Tables:" + t.getNameAsString());
        }
        admin.close();
        logger.info("ooo DB ooo TABLES CREATED ooo DB ooo");
    }
}
