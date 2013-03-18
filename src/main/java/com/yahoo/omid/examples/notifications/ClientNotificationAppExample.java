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
import java.util.concurrent.CountDownLatch;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.yahoo.omid.client.TransactionManager;
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.client.TransactionalTable;
import com.yahoo.omid.notifications.Interest;
import com.yahoo.omid.notifications.client.DeltaOmid;
import com.yahoo.omid.notifications.client.IncrementalApplication;
import com.yahoo.omid.notifications.client.Observer;

/**
 * This applications shows the basic usage of the Omid's notification framework
 * 
 */
public class ClientNotificationAppExample {

    private static final Logger logger = Logger.getLogger(ClientNotificationAppExample.class);

    private static CountDownLatch cdl;

    /**
     * Launches ObserverRegistrationService and perform an observer registration
     * 
     * @param args
     */
    @SuppressWarnings("static-access")
    public static void main(String[] args) throws Exception {

        CommandLineParser cmdLineParser = new ExtendedPosixParser(true);

        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("txs").withDescription("Number of transactions to execute")
                .withType(Number.class).hasArg().withArgName("argname").create());
        options.addOption(OptionBuilder.withLongOpt("rows-per-tx")
                .withDescription("Number of rows that each transaction inserts").withType(Number.class).hasArg()
                .withArgName("argname").create());

        int txsToExecute = 1; // Default value
        int rowsPerTx = 1; // Default value

        try {
            CommandLine cmdLine = cmdLineParser.parse(options, args);

            if (cmdLine.hasOption("txs")) {
                txsToExecute = ((Number) cmdLine.getParsedOptionValue("txs")).intValue();
            }

            if (cmdLine.hasOption("rows-per-tx")) {
                rowsPerTx = ((Number) cmdLine.getParsedOptionValue("rows-per-tx")).intValue();
            }

            cdl = new CountDownLatch(txsToExecute * rowsPerTx * 2);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }

        // TSO Client setup
        Configuration tsoClientHbaseConf = HBaseConfiguration.create();
        tsoClientHbaseConf.set("tso.host", "localhost");
        tsoClientHbaseConf.setInt("tso.port", 1234);

        logger.info("ooo Omid ooo - STARTING OMID'S EXAMPLE NOTIFICATION APP. - ooo Omid ooo");

        logger.info("ooo Omid ooo - TABLE " + TABLE_1 + " SHOULD EXISTS WITH CF " + COLUMN_FAMILY_1 + "- ooo Omid ooo");

        Observer obs1 = new Observer() {

            Interest interestObs1 = new Interest(TABLE_1, COLUMN_FAMILY_1, COLUMN_1);

            public void onColumnChanged(byte[] column, byte[] columnFamily, byte[] table, byte[] rowKey,
                    TransactionState tx) {
                logger.info("ooo Omid ooo -" + "I'M OBSERVER o1." + " An update has occurred on Table: "
                        + Bytes.toString(table) + " RowKey: " + Bytes.toString(rowKey) + " ColumnFamily: "
                        + Bytes.toString(columnFamily) + " Column: " + Bytes.toString(column) + " !!! - ooo Omid ooo");
                Configuration tsoClientConf = HBaseConfiguration.create();
                tsoClientConf.set("tso.host", "localhost");
                tsoClientConf.setInt("tso.port", 1234);

                TransactionalTable tt = null;
                try {
                    tt = new TransactionalTable(tsoClientConf, TABLE_1);
                    doTransactionalPut(tx, tt, rowKey, Bytes.toBytes(COLUMN_FAMILY_1), Bytes.toBytes(COLUMN_2),
                            Bytes.toBytes("Data written by OBSERVER o1"));
                    logger.info("ooo Omid ooo - o1 INSERTED ROW ON COL " + COLUMN_2 + " (TX " + tx + ") - ooo Omid ooo");
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
                cdl.countDown();
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
                logger.info("ooo Omid ooo - " + "I'M OBSERVER o2." + " An update has occurred on Table: "
                        + Bytes.toString(table) + " RowKey: " + Bytes.toString(rowKey) + " ColumnFamily: "
                        + Bytes.toString(columnFamily) + " Column: " + Bytes.toString(column)
                        + " !!! I'M NOT GONNA DO ANYTHING ELSE - ooo Omid ooo");
                cdl.countDown();
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
        final IncrementalApplication app = new DeltaOmid.AppBuilder("ExampleApp", 6666).addObserver(obs1)
                .addObserver(obs2).build();

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

        logger.info("ooo Omid ooo - WAITING 10 SECONDS TO ALLOW OBSERVER REGISTRATION - ooo Omid ooo");
        Thread.currentThread().sleep(10000);

        TransactionManager tm = new TransactionManager(tsoClientHbaseConf);
        TransactionalTable tt = new TransactionalTable(tsoClientHbaseConf, TABLE_1);

        logger.info("ooo Omid ooo - STARTING " + txsToExecute + " TRIGGER TXS INSERTING " + rowsPerTx
                + " ROWS EACH IN COLUMN " + COLUMN_1 + " - ooo Omid ooo");
        for (int i = 0; i < txsToExecute; i++) {
            // Transaction adding to rows to a table
            TransactionState tx = tm.beginTransaction();

            for (int j = 0; j < rowsPerTx; j++) {
                doTransactionalPut(tx, tt, Bytes.toBytes("row-" + Integer.toString(i + (j * 10000))),
                        Bytes.toBytes(COLUMN_FAMILY_1), Bytes.toBytes(COLUMN_1),
                        Bytes.toBytes("testWrite-" + Integer.toString(i + (j * 10000))));
            }

            tm.tryCommit(tx);
        }
        tt.close();

        logger.info("ooo Omid ooo - TRIGGER TXS COMMITTED. WAITING FOR THE 2 OBS RECEIVE ALL NOTIF - ooo Omid ooo");
        cdl.await();
        logger.info("ooo Omid ooo - OBSERVERS HAVE RECEIVED ALL THE NOTIFICATIONS. FINISHING APP - ooo Omid ooo");
        Thread.currentThread().sleep(30000);
        app.close();

        logger.info("ooo Omid ooo - OMID'S NOTIFICATION APP FINISHED - ooo Omid ooo");
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

}
