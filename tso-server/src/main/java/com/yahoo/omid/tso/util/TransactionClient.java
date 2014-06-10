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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.NullCommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.tso.util.ClientHandler.RowDistribution;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Transaction Client using Serialization
 */
public class TransactionClient {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionClient.class);

    public static void main(String[] args) throws Exception {
        
        final Config config = Config.parseConfig(args);
        List<ClientHandler> handlers = new ArrayList<ClientHandler>();

        Configuration conf = new BaseConfiguration();
        conf.setProperty("tso.host", config.tsoHost);
        conf.setProperty("tso.port", config.tsoPort);
        conf.setProperty("request.timeout-ms", -1);
        
        LOG.info("Starting {} clients with the following configuration:", config.nbClients);
        LOG.info("PARAM MAX_ROW: {}", config.maxTxSize);
        
        float readPercentage = config.readproportion==-1?config.percentReads:(config.readproportion * 100);
        
        LOG.info("readPercent {}", config.percentReads);
        
        RowDistribution rowDistribution = RowDistribution.valueOf(config.requestDistribution.toUpperCase());
        IntegerGenerator[] intGenerators = new IntegerGenerator[config.nbClients];
        
        LOG.info("Initializing cell id generators for distribution [{}] (that may take a while)",
                config.requestDistribution);

        // zipfian generator takes a while to initialize. Do that first
        for(int i = 0; i < intGenerators.length; ++i) {
            if (rowDistribution.equals(RowDistribution.UNIFORM)) {
                intGenerators[i] = new IntegerGenerator() {
                        Random r = new Random();
                        @Override
                        public int nextInt() {
                            return r.nextInt(Integer.MAX_VALUE);
                        }
                    
                        @Override
                        public double mean() {
                            // TODO Auto-generated method stub
                            return 0;
                        }
                    };
            } else {
                intGenerators[i] = new ScrambledZipfianGenerator(Long.MAX_VALUE);
            }
        }

        MetricRegistry metrics = new MetricRegistry();
        final Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
            .outputTo(LoggerFactory.getLogger("metrics"))
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
        reporter.start(10, TimeUnit.SECONDS);

        
        
        for(int i = 0; i < config.nbClients; ++i) {
            CommitTable commitTable;
            if (config.isHBase()) {
                commitTable = new HBaseCommitTable(HBaseConfiguration.create(),
                                                   config.getHBaseCommitTable());
            } else {
                commitTable = new NullCommitTable();
            }
            // Create the associated Handler
            ClientHandler handler = new ClientHandler(conf,
                                                      commitTable.getClient().get(),
                                                      metrics,
                                                      config.runFor, config.nbMessages,
                                                      config.maxInFlight, config.commitDelay,
                                                      readPercentage, config.maxTxSize,
                                                      intGenerators[i]);
            // *** Start the Netty running ***
            handlers.add(handler);
        }

        
        Thread.sleep(config.runFor * 1000);

        for (ClientHandler handler : handlers) {
            handler.shutdown();
        }
        
        LOG.info("Benchmark complete - please check the metrics from individual clients in the log");
    }
    
    
    private static class Config {
        
        static public Config parseConfig(String args[]) {
            Config config = new Config();
            new JCommander(config, args);
            return config;
        }
        
        @Parameter(names = "-tsoHost", description = "Hostname of the Status Oracle")
        String tsoHost = "localhost";
        
        @Parameter(names = "-tsoPort", description = "Port reserved by the Status Oracle")
        int tsoPort = 54758;
        
        @Parameter(names = "-tsoExecutorThreads", description = "Concurrent netty client threads", required = false)
        int executorThreads = 10;

        @Parameter(names = "-runFor", description = "Number of seconds to run for")
        int runFor = 600;

        @Parameter(names= "-nbMessages", description = "Maximum number of messages to send")
        long nbMessages = Long.MAX_VALUE;
        
        @Parameter(names="-maxInFlight", description="Max number of outstanding messages in the TSO pipe")
        int maxInFlight = 100000;
        
        @Parameter(names="-percentRead", description="% reads")
        float percentReads = 0;
        
        @Parameter(names="-readProportion", description="proportion of reads, between 1 and 0, overrides -percentRead if specified", hidden = true)
        float readproportion = -1;
        
        @Parameter(names="-nbClients", description="Number of TSO clients")
        int nbClients = 1;
        
        @Parameter(names="-commitDelay", description="Number of milliseconds to delay between acquiring timestamp and committing")
        int commitDelay = 50;
        
        @Parameter(names="-maxTxSize", description="Maximum size of transaction (size homogeneously distributed between 1 and this number)")
        int maxTxSize = ClientHandler.DEFAULT_MAX_ROW;
        
        @Parameter(names="-requestDistribution", description="Request distribution (how to pick rows) {uniform|zipfian}"  )
        String requestDistribution = "uniform";
        
        @Parameter(names = "-hbase", description = "Enable HBase storage")
        private boolean hbase = false;

        @Parameter(names = "-hbaseCommitTable", description = "HBase commit table name")
        private String hbaseCommitTable = HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME;
        
        public boolean isHBase() {
            return hbase;
        }
        
        public String getHBaseCommitTable() {
            return hbaseCommitTable;
        }
    }
}
