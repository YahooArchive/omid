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

package com.yahoo.omid.tso;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * Holds the configuration parameters of a TSO server instance.
 * 
 */
public class TSOServerConfig {


    // used for testing
    static public TSOServerConfig configFactory(int port, int batch, boolean recoveryEnabled, int ensSize, int qSize,
            String zkservers) {
        TSOServerConfig config = new TSOServerConfig();
        config.port = port;
        config.batch = batch;
        config.recoveryEnabled = recoveryEnabled;
        config.ensemble = ensSize;
        config.quorum = qSize;
        config.zkServers = zkservers;
        return config;
    }

    static public TSOServerConfig parseConfig(String args[]) {
        TSOServerConfig config = new TSOServerConfig();

        if (args.length == 0) {
            new JCommander(config).usage();
            System.exit(0);
        }

        new JCommander(config, args);

        return config;
    }

    @Parameter(names = "-port", description = "Port reserved by the Status Oracle", required = true)
    private int port;

    @Parameter(names = "-batch", description = "Number of bytes to batch before flushing to WAL and sending responses.")
    private int batch;

    @Parameter(names = "-ha", description = "Highly Available status oracle: logs operations to the WAL and recovers from a crash")
    private boolean recoveryEnabled;

    @Parameter(names = "-zk", description = "ZooKeeper ensemble: host1:port1,host2:port2...")
    private String zkServers;

    @Parameter(names = "-ensemble", description = "WAL ensemble size")
    private int ensemble;

    @Parameter(names = "-quorum", description = "WAL quorum size")
    private int quorum;

    @Parameter(names = "-fsLog", description = "local FS WAL directory")
    private String fsLog;

    @Parameter(names = "-metrics", description = "Metrics config")
    private String metrics;
    
    @Parameter(names = "-maxCommits", description = "Size of commit list")
    private int maxCommits = 1000000;
    
    @Parameter(names = "-maxItems", description = "Maximum number of items in the TSO (will determine the 'low watermark')")
    private int maxItems = 1000000;
    
    @Parameter(names = "-flushTimeout", description = "Maximum delay before flushing batch of replies and sending responses, unit is [ms]")
    private int flushTimeout = 10;
    
    
    public int getPort() {
        return port;
    }

    public int getBatchSize() {
        return batch;
    }

    public boolean isRecoveryEnabled() {
        return recoveryEnabled;
    }

    public String getZkServers() {
        return zkServers;
    }

    public int getEnsembleSize() {
        return ensemble;
    }

    public int getQuorumSize() {
        return quorum;
    }

    public String getMetrics() {
        return metrics;
    }

    public String getFsLog() {
        return fsLog;
    }
    
    public int getMaxCommits() {
        return maxCommits;
    }

    public int getMaxItems() {
        return maxItems;
    }

    public int getFlushTimeout() {
        return flushTimeout;
    }

}
