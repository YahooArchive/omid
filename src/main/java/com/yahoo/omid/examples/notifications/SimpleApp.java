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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import com.google.common.net.HostAndPort;
import com.yahoo.omid.examples.notifications.ExamplesUtils.ExtendedPosixParser;
import com.yahoo.omid.notifications.Interest;
import com.yahoo.omid.notifications.client.DeltaOmid;
import com.yahoo.omid.notifications.client.IncrementalApplication;
import com.yahoo.omid.notifications.client.Observer;
import com.yahoo.omid.notifications.conf.ClientConfiguration;
import com.yahoo.omid.transaction.Transaction;

/**
 * This applications shows the basic usage of the Omid's notification framework
 * 
 */
public class SimpleApp {

    private static final Logger logger = Logger.getLogger(SimpleApp.class);

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
        options.addOption(OptionBuilder.withArgName("omid_server").hasArg().withDescription("omid server: host1:port1")
                .create("omid"));

        int appInstancePort = 16666;
        String zk = "localhost:2181";
        String omid = "localhost:1234";

        try {
            CommandLine cmdLine = cmdLineParser.parse(options, args);

            if (cmdLine.hasOption("port")) {
                appInstancePort = ((Number) cmdLine.getParsedOptionValue("port")).intValue();
            }

            if (cmdLine.hasOption("zk")) {
                zk = cmdLine.getOptionValue("zk");
            }

            if (cmdLine.hasOption("omid")) {
                omid = cmdLine.getOptionValue("omid");
            }

        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }

        final HostAndPort omidHp = HostAndPort.fromString(omid);

        logger.info("ooo SimpleApp ooo - STARTING OMID'S EXAMPLE NOTIFICATION APP. - ooo SimpleApp ooo");

        logger.info("ooo SimpleApp ooo - TABLE " + TABLE_1 + " SHOULD EXISTS WITH CF " + COLUMN_FAMILY_1
                + "- ooo SimpleApp ooo");

        Observer obs1 = new Observer() {

            Interest interestObs1 = new Interest(TABLE_1, COLUMN_FAMILY_1, COLUMN_1);

            // private ThreadLocal<TTable> txTable = new ThreadLocal<TTable>() {
            //
            // @Override
            // protected TTable initialValue() {
            // // TSO Client setup
            // Configuration tsoClientHbaseConfObs = HBaseConfiguration.create();
            // tsoClientHbaseConfObs.set("tso.host", omidHp.getHostText());
            // tsoClientHbaseConfObs.setInt("tso.port", omidHp.getPortOrDefault(1234));
            // try {
            // logger.info("Returning new " + TABLE_1 + " txtable");
            // return new TTable(tsoClientHbaseConfObs, TABLE_1);
            // } catch (IOException e) {
            // throw new RuntimeException("Cannot create transactional table on content table", e);
            // }
            // }
            //
            // };

            public void onInterestChanged(Result rowData, Transaction tx) {
                logger.info("o1 -> Update on " + rowData.list());

                // try {
                // ExamplesUtils.doTransactionalPut(tx, txTable.get(), rowData.getRow(),
                // Bytes.toBytes(COLUMN_FAMILY_1), Bytes.toBytes(COLUMN_2),
                // Bytes.toBytes("data written by observer o1"));
                // } catch (IOException e) {
                // e.printStackTrace();
                // }
            }

            @Override
            public String getName() {
                return "o1";
            }

            @Override
            public Interest getInterest() {
                return interestObs1;
            }
        };

        Observer obs2 = new Observer() {

            Interest interestObs2 = new Interest(TABLE_1, COLUMN_FAMILY_1, COLUMN_2);

            public void onInterestChanged(Result rowData, Transaction tx) {
                logger.info("o2 -> Update on " + rowData.list());
            }

            @Override
            public String getName() {
                return "o2";
            }

            @Override
            public Interest getInterest() {
                return interestObs2;
            }
        };

        // Create application
        ClientConfiguration cf = new ClientConfiguration();
        cf.setOmidServer(omid);
        cf.setZkServers(zk);
        final IncrementalApplication app = new DeltaOmid.AppBuilder("SimpleApp", appInstancePort).setConfiguration(cf)
                .addObserver(obs1).build();

        logger.info("ooo SimpleApp ooo - APP INSTANCE CREATED - ooo SimpleApp ooo");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    app.close();
                    logger.info("ooo SimpleApp ooo - Omid's Notification Example App Stopped (CTRL+C) - ooo SimpleApp ooo");
                } catch (IOException e) {
                    // Ignore
                }
            }
        });

    }

}
