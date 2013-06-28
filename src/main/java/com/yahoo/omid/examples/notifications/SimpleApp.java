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
import static com.yahoo.omid.examples.Constants.LATENCY_TS;
import static com.yahoo.omid.examples.Constants.TABLE_1;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.yahoo.omid.examples.notifications.ExamplesUtils.ExtendedPosixParser;
import com.yahoo.omid.notifications.Interest;
import com.yahoo.omid.notifications.client.DeltaOmid;
import com.yahoo.omid.notifications.client.IncrementalApplication;
import com.yahoo.omid.notifications.client.Observer;
import com.yahoo.omid.notifications.conf.ClientConfiguration;
import com.yahoo.omid.transaction.Transaction;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;

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

        logger.info("ooo SimpleApp ooo - STARTING OMID'S EXAMPLE NOTIFICATION APP. - ooo SimpleApp ooo");

        logger.info("ooo SimpleApp ooo - TABLE " + TABLE_1 + " SHOULD EXISTS WITH CF " + COLUMN_FAMILY_1
                + "- ooo SimpleApp ooo");

        final Timer o1Timer = Metrics.newTimer(SimpleApp.class, "SimpleApp@latency-o1", TimeUnit.MILLISECONDS,
                TimeUnit.SECONDS);
        final Timer o1TotalTimer = Metrics.newTimer(SimpleApp.class, "SimpleApp@latency-o1-total",
                TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

        Observer obs1 = new Observer() {

            Interest interestObs1 = new Interest(TABLE_1, COLUMN_FAMILY_1, COLUMN_1);

            public void onInterestChanged(Result rowData, Transaction tx) {
                logger.debug("o1 -> Update on " + rowData.list());
                KeyValue timestampKV = rowData.getColumnLatest(Bytes.toBytes(COLUMN_FAMILY_1),
                        Bytes.toBytes(LATENCY_TS));
                if (timestampKV != null) {
                    LatencyUtils.measureLatencies(o1TotalTimer, o1Timer, timestampKV);
                } else {
                    logger.warn("No timestamp2 for " + Bytes.toString(rowData.getRow()));
                }
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
