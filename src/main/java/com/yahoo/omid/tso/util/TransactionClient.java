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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.yahoo.omid.tso.util.ClientHandler.RowDistribution;

/**
 * Simple Transaction Client using Serialization
 * @author maysam
 *
 */
public class TransactionClient {

    public static void main(String[] args) throws Exception {

        // *** Start the Netty configuration ***

        final Config config = Config.parseConfig(args);
        List<ClientHandler> handlers = new ArrayList<ClientHandler>();

        Configuration conf = HBaseConfiguration.create();
        conf.set("tso.host", config.tsoHost);
        conf.setInt("tso.port", config.tsoPort);
        conf.setInt("tso.executor.threads", 10);
        
        System.out.println("Starting " + config.nbClients + " clients with the following configuration:");
        System.out.println("PARAM MAX_ROW: " + config.maxTxSize);
        System.out.println("PARAM DB_SIZE: " + config.dbSize);
        System.out.println("pause " + config.pause);
        
        float readPercentage = config.readproportion==-1?config.percentReads:(config.readproportion * 100);
        
        System.out.println("readPercent " + config.percentReads);
        
        RowDistribution rowDistribution = RowDistribution.valueOf(config.requestDistribution.toUpperCase());
        IntegerGenerator[] intGenerators = new IntegerGenerator[config.nbClients];
        
        System.out.println("Initializing row ids generators for distribution ["+config.requestDistribution + "] (that may take a while)");
        // zipfian generator takes a while to initialize. Do that first
        for(int i = 0; i < intGenerators.length; ++i) {
            if (rowDistribution.equals(RowDistribution.UNIFORM)) {
                intGenerators[i] = new IntegerGenerator() {
                    Random r = new Random();
                    @Override
                    public int nextInt() {
                        return r.nextInt(config.dbSize);
                    }
                    
                    @Override
                    public double mean() {
                        // TODO Auto-generated method stub
                        return 0;
                    }
                };
            } else {
                intGenerators[i] = new ScrambledZipfianGenerator(config.dbSize);
            }
        }

        for(int i = 0; i < config.nbClients; ++i) {
         // Create the associated Handler
            ClientHandler handler = new ClientHandler(conf, config.runFor, config.nbMessages,
                                                      config.maxInFlight, config.pause,
                                                      readPercentage, config.maxTxSize,
                                                      config.dbSize, intGenerators[i]);
   
           // *** Start the Netty running ***
           handlers.add(handler);
        }
        
        // Wait for the Traffic to finish
        for (ClientHandler handler : handlers) {
           handler.waitForAll();
        }
        
        System.out.println("\n**********\nBenchmark complete - please check the metrics from individual client threads in the console / log");

        // NOTE: for simplicity we don't properly close netty channels or release resources in this example. 
        
        System.exit(0);
    }
    
    
    private static class Config {
        
        static public Config parseConfig(String args[]) {
            Config config = new Config();

            if (args.length == 0) {
                new JCommander(config).usage();
                System.exit(0);
            }

            new JCommander(config, args);

            return config;
        }
        
        @Parameter(names = "-tsoHost", description = "Hostname of the Status Oracle")
        String tsoHost = "localhost";
        
        @Parameter(names = "-tsoPort", description = "Port reserved by the Status Oracle")
        int tsoPort = 1234;
        
        @Parameter(names = "-tsoExecutorThreads", description = "Concurrent netty client threads", required = false)
        int executorThreads = 10;

        @Parameter(names = "-runFor", description = "Number of seconds to run for")
        int runFor = 600;

        @Parameter(names= "-nbMessages", description = "Maximum number of messages to send")
        long nbMessages = Long.MAX_VALUE;
        
        @Parameter(names="-maxInFlight", description="Max number of outstanding messages in the TSO pipe")
        int maxInFlight = 100;
        
        @Parameter(names="-percentRead", description="% reads")
        float percentReads = 0;
        
        @Parameter(names="-readProportion", description="proportion of reads, between 1 and 0, overrides -percentRead if specified", hidden = true)
        float readproportion = -1;
        
        @Parameter(names="-nbClients", description="Number of TSO clients")
        int nbClients = 1;
        
        @Parameter(names="-pause", description="Pause client for " + ClientHandler.PAUSE_LENGTH + "ms after sending a commit request")
        boolean pause = false;
        
        @Parameter(names="-maxTxSize", description="Maximum size of transaction (size homogeneously distributed between 1 and this number)")
        int maxTxSize = ClientHandler.DEFAULT_MAX_ROW;
        
        @Parameter(names="-dbSize", description="Total number of rows in the database")
        int dbSize = ClientHandler.DEFAULT_DB_SIZE;
        
        @Parameter(names="-requestDistribution", description="Request distribution (how to pick rows) {uniform|zipfian}"  )
        String requestDistribution = "uniform";
        
    }
}
