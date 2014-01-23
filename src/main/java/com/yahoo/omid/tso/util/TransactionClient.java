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
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.yahoo.omid.tso.TSOServerConfig;


/**
 * Simple Transaction Client using Serialization
 * @author maysam
 *
 */
public class TransactionClient {

    /**
     * Main class for Client taking from two to four arguments<br>
     * -host for server<br>
     * -port for server<br>
     * -number of message (default is 256)<br>
     * -MAX_IN_FLIGHT
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Print usage if no argument is specified.
//        if (args.length < 2 || args.length > 7) {
//            System.err
//                    .println("Usage: " +
//                            TransactionClient.class.getSimpleName() +
//                            " <host> <port> [<number of messages>] [<MAX_IN_FLIGHT>] [<runs>] [<pause>] [<% reads>]");
//            return;
//        }
//
//        // Parse options.
//        String host = args[0];
//        int port = Integer.parseInt(args[1]);
//        int nbMessage;
//
//        if (args.length >= 3) {
//            nbMessage = Integer.parseInt(args[2]);
//        } else {
//            nbMessage = 256;
//        }
//        int inflight = 10;
//        if (args.length >= 4) {
//            inflight = Integer.parseInt(args[3]);
//        }
//        
//        int runs = 1;
//        if (args.length >= 5) {
//           runs = Integer.parseInt(args[4]);
//        }
//
//        boolean pauseClient = false;
//        if (args.length >= 6) {
//           pauseClient = Boolean.parseBoolean(args[5]);
//        }
//        
//        float percentRead = 0;
//        if (args.length >= 7) {
//           percentRead = Float.parseFloat(args[6]);
//        }

        // *** Start the Netty configuration ***

        Config config = Config.parseConfig(args);
        List<ClientHandler> handlers = new ArrayList<ClientHandler>();

        Configuration conf = HBaseConfiguration.create();
        conf.set("tso.host", config.tsoHost);
        conf.setInt("tso.port", config.tsoPort);
        conf.setInt("tso.executor.threads", 10);
        
        System.out.println("Starting " + config.nbRuns + " clients with the following configuration:");
        System.out.println("PARAM MAX_ROW: " + config.maxTxSize);
        System.out.println("PARAM DB_SIZE: " + config.dbSize);
        System.out.println("pause " + config.pause);
        
        float readPercentage = config.readproportion==-1?config.percentReads:(config.readproportion * 100);
        
        System.out.println("readPercent " + config.percentReads);

        for(int i = 0; i < config.nbRuns; ++i) {
         // Create the associated Handler
           ClientHandler handler = new ClientHandler(conf, config.nbMessages, config.inFlight, config.pause, readPercentage, config.maxTxSize, config.dbSize);
   
           // *** Start the Netty running ***
           handlers.add(handler);
        }
        
        // Wait for the Traffic to finish
        for (ClientHandler handler : handlers) {
           handler.waitForAll();
        }
        
        System.out.println("\n**********\nBenchmark complete - please check the metrics from individual client threads in the console / log");

        // NOTE: for simplicity we don't properly close netty channels or release resources in this example. 
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
        
        @Parameter(names = "-tsoHost", description = "Hostname of the Status Oracle", required = true)
        String tsoHost;
        
        @Parameter(names = "-tsoPort", description = "Port reserved by the Status Oracle", required = true)
        int tsoPort =-1;
        
        @Parameter(names = "-tsoExecutorThreads", description = "Concurrent netty client threads", required = false)
        int executorThreads = 10;

        @Parameter(names= "-nbMessages", description = "Number of messages to send")
        int nbMessages = 100000;
        
        @Parameter(names="-nbOutstanding", description="Number of outstanding messages in the TSO pipe")
        int inFlight = 100;
        
        @Parameter(names="-percentRead", description="% reads")
        float percentReads = 0;
        
        @Parameter(names="-readproportion", description="proportion of reads, between 1 and 0, overrides -percentRead if specified", hidden = true)
        float readproportion = -1;
        
        @Parameter(names="-nbRuns", description="Number of iterations")
        int nbRuns = 1;
        
        @Parameter(names="-pause", description="Pause client for " + ClientHandler.PAUSE_LENGTH + "ms after sending a commit request")
        boolean pause = false;
        
        @Parameter(names="-maxTxSize", description="Maximum size of transaction (size homogeneously distributed between 1 and this number)")
        int maxTxSize = ClientHandler.DEFAULT_MAX_ROW;
        
        @Parameter(names="-dbSize", description="Total number of rows in the database")
        int dbSize = ClientHandler.DEFAULT_DB_SIZE;
        
    }
}
