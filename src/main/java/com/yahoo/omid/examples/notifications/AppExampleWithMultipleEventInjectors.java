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
import static com.yahoo.omid.examples.Constants.COLUMN_2;
import static com.yahoo.omid.examples.Constants.COLUMN_FAMILY_1;
import static com.yahoo.omid.examples.Constants.TABLE_1;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
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
import org.apache.commons.cli.PosixParser;
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
import com.yahoo.omid.notifications.Interest;
import com.yahoo.omid.notifications.TransactionCommittedRegionCoprocessor;
import com.yahoo.omid.notifications.client.DeltaOmid;
import com.yahoo.omid.notifications.client.IncrementalApplication;
import com.yahoo.omid.notifications.client.Observer;
import com.yahoo.omid.notifications.conf.ClientConfiguration;

/**
 * This applications shows the basic usage of the Omid's notification framework
 * 
 */
public class AppExampleWithMultipleEventInjectors {

    private static final Logger logger = Logger.getLogger(AppExampleWithMultipleEventInjectors.class);

    /**
     * Launches ObserverRegistrationService and perform an observer registration
     * 
     * @param args
     */
    @SuppressWarnings("static-access")
    public static void main(String[] args) throws Exception {

        CommandLineParser cmdLineParser = new ExtendedPosixParser(true);

        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("port")
                .withDescription("app instance port to receive notifications").withType(Number.class).hasArg()
                .withArgName("argname").create());
        options.addOption(OptionBuilder.withArgName("zk_cluster").hasArg()
                .withDescription("comma separated zk server list: host1:port1,host2:port2...").create("zk"));
        options.addOption(OptionBuilder.withArgName("hbase_master").hasArg()
                .withDescription("hbase master server: host1:port1").create("hbase"));
        options.addOption("createDB", false, "If present, the database will be re-created");
        options.addOption(OptionBuilder.withArgName("omid_server").hasArg().withDescription("omid server: host1:port1")
                .create("omid"));
        options.addOption("inject", false, "If present, load will be injected");
        options.addOption(OptionBuilder.withLongOpt("injectors").withDescription("Number of tasks to inject txs")
                .withType(Number.class).hasArg().withArgName("argname").create());
        options.addOption(OptionBuilder.withLongOpt("tx-rate")
                .withDescription("Number of txs/s to execute per injector thread").withType(Number.class).hasArg()
                .withArgName("argname").create());

        boolean inject = false;
        boolean createDB = false;
        int nOfInjectorTasks = 1;
        int txRate = 1;
        int appInstancePort = 6666;
        String zk = "localhost:2181";
        String hbase = "localhost:2181";
        String omid = "localhost:1234";

        try {
            CommandLine cmdLine = cmdLineParser.parse(options, args);

            if (cmdLine.hasOption("port")) {
                appInstancePort = ((Number) cmdLine.getParsedOptionValue("port")).intValue();
            }

            if (cmdLine.hasOption("zk")) {
                zk = cmdLine.getOptionValue("zk");
            }

            if (cmdLine.hasOption("hbase")) {
                hbase = cmdLine.getOptionValue("hbase");
            }

            if (cmdLine.hasOption("omid")) {
                omid = cmdLine.getOptionValue("omid");
            }

            if (cmdLine.hasOption("createDB")) {
                createDB = true;
            }

            if (cmdLine.hasOption("inject")) {
                inject = true;
            }

            if (cmdLine.hasOption("injectors")) {
                nOfInjectorTasks = ((Number) cmdLine.getParsedOptionValue("injectors")).intValue();
            }

            if (cmdLine.hasOption("tx-rate")) {
                txRate = ((Number) cmdLine.getParsedOptionValue("tx-rate")).intValue();
            }
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // TSO Client setup
        Configuration hbaseConf = HBaseConfiguration.create();
        final HostAndPort hbaseHp = HostAndPort.fromString(hbase);
        hbaseConf.set("hbase.zookeeper.quorum", hbaseHp.getHostText());
        hbaseConf.set("hbase.zookeeper.property.clientPort", Integer.toString(hbaseHp.getPortOrDefault(2181)));

        if (createDB) {
            createTables(hbaseConf);
        }

        Configuration tsoClientConf = HBaseConfiguration.create();
        final HostAndPort omidHp = HostAndPort.fromString(omid);
        tsoClientConf.set("hbase.zookeeper.quorum", hbaseHp.getHostText());
        tsoClientConf.set("hbase.zookeeper.property.clientPort", Integer.toString(hbaseHp.getPortOrDefault(2181)));
        tsoClientConf.set("tso.host", omidHp.getHostText());
        tsoClientConf.setInt("tso.port", omidHp.getPortOrDefault(1234));
        logger.trace(omidHp.getHostText() + " " + omidHp.getPortOrDefault(1234));

        logger.info("ooo Omid ooo - STARTING OMID'S EXAMPLE NOTIFICATION APP. - ooo Omid ooo");

        logger.info("ooo Omid ooo - TABLE " + TABLE_1 + " SHOULD EXISTS WITH CF " + COLUMN_FAMILY_1 + "- ooo Omid ooo");

        Observer obs1 = new Observer() {

            Interest interestObs1 = new Interest(TABLE_1, COLUMN_FAMILY_1, COLUMN_1);

            public void onColumnChanged(byte[] column, byte[] columnFamily, byte[] table, byte[] rowKey,
                    TransactionState tx) {
                // logger.info("o1 -> Update on " + Bytes.toString(table) + Bytes.toString(rowKey)
                // + Bytes.toString(columnFamily) + Bytes.toString(column));

                TransactionalTable tt = null;
                try {
                    Configuration tsoClientHbaseConfObs = HBaseConfiguration.create();
                    tsoClientHbaseConfObs.set("hbase.zookeeper.quorum", hbaseHp.getHostText());
                    tsoClientHbaseConfObs.set("hbase.zookeeper.property.clientPort",
                            Integer.toString(hbaseHp.getPortOrDefault(2181)));
                    tsoClientHbaseConfObs.set("tso.host", omidHp.getHostText());
                    tsoClientHbaseConfObs.setInt("tso.port", omidHp.getPortOrDefault(1234));

                    tt = new TransactionalTable(tsoClientHbaseConfObs, TABLE_1);
                    doTransactionalPut(tx, tt, rowKey, Bytes.toBytes(COLUMN_FAMILY_1), Bytes.toBytes(COLUMN_2),
                            Bytes.toBytes("data written by observer o1"));
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (tt != null) {
                            tt.close();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public String getName() {
                return "o1";
            }

            @Override
            public List<Interest> getInterests() {
                return Collections.singletonList(interestObs1);
            }
        };

        Observer obs2 = new Observer() {

            Interest interestObs2 = new Interest(TABLE_1, COLUMN_FAMILY_1, COLUMN_2);

            public void onColumnChanged(byte[] column, byte[] columnFamily, byte[] table, byte[] rowKey,
                    TransactionState tx) {
                // logger.info("o2 -> Update on " + Bytes.toString(table) + Bytes.toString(rowKey)
                // + Bytes.toString(columnFamily) + Bytes.toString(column));
            }

            @Override
            public String getName() {
                return "o2";
            }

            @Override
            public List<Interest> getInterests() {
                return Collections.singletonList(interestObs2);
            }
        };

        // Create application
        ClientConfiguration cf = new ClientConfiguration();
        cf.setOmidServer(omid);
        cf.setZkServers(zk);
        final IncrementalApplication app = new DeltaOmid.AppBuilder("ExampleApp", appInstancePort).setConfiguration(cf)
                .addObserver(obs1).addObserver(obs2).build();

        logger.info("ooo Omid ooo - APP Instance Created - ooo Omid ooo");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    app.close();
                    logger.info("ooo Omid ooo - Omid's Notification Example App Stopped (CTRL+C) - ooo Omid ooo");
                } catch (IOException e) {
                    // Ignore
                }
            }
        });

        if (inject) {
            logger.info("ooo Omid ooo - WAITING 10 SECONDS TO ALLOW OBSERVER REGISTRATION - ooo Omid ooo");
            Thread.currentThread().sleep(10000);

            ExecutorService injectors = Executors.newFixedThreadPool(nOfInjectorTasks, new ThreadFactoryBuilder()
                    .setNameFormat("Injector-%d").build());
            CountDownLatch startCdl = new CountDownLatch(1);
            for (int i = 0; i < nOfInjectorTasks; i++) {
                injectors.execute(new EventInjectorTask(tsoClientConf, startCdl, txRate));
            }
            logger.info("ooo Omid ooo - STARTING " + nOfInjectorTasks + " LOOP TASKS INJECTING AT " + txRate
                    + " TX/S IN COLUMN " + COLUMN_1 + " - ooo Omid ooo");
            startCdl.countDown();
            logger.info("ooo Omid ooo - OMID'S NOTIFICATION APP INJECTING LOAD TILL STOPPED - ooo Omid ooo");
        }

    }

    private static class EventInjectorTask implements Runnable {

        private CountDownLatch startCdl;
        private TransactionManager tm;
        private TransactionalTable tt;
        private int txRate;

        public EventInjectorTask(Configuration conf, CountDownLatch startCdl, int txRate) {
            try {
                this.tm = new TransactionManager(conf);
                this.tt = new TransactionalTable(conf, TABLE_1);
            } catch (Exception e) {
                e.printStackTrace();
            }
            this.startCdl = startCdl;
            this.txRate = txRate;
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
    }

    private static class ExtendedPosixParser extends PosixParser {

        private boolean ignoreUnrecognizedOption;

        public ExtendedPosixParser(final boolean ignoreUnrecognizedOption) {
            this.ignoreUnrecognizedOption = ignoreUnrecognizedOption;
        }

        @Override
        protected void processOption(final String arg, final ListIterator iter) throws ParseException {
            boolean hasOption = getOptions().hasOption(arg);

            if (hasOption || !ignoreUnrecognizedOption) {
                super.processOption(arg, iter);
            }
        }

    }

    private static void doTransactionalPut(TransactionState tx, TransactionalTable tt, byte[] rowName,
            byte[] colFamName, byte[] colName, byte[] dataValue) throws IOException {
        Put row = new Put(rowName);
        row.add(colFamName, colName, dataValue);
        tt.put(tx, row);
    }

    private static void createTables(Configuration hbaseConf) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException {
        logger.info("Re-Creating Tables");
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
    }
}
