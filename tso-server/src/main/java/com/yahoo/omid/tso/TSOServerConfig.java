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
import com.beust.jcommander.IVariableArity;

import java.util.List;
import java.util.ArrayList;

/**
 * Holds the configuration parameters of a TSO server instance.
 * 
 */
public class TSOServerConfig implements IVariableArity {


    // used for testing
    static public TSOServerConfig configFactory(int port, int maxItems) {
        TSOServerConfig config = new TSOServerConfig();
        config.port = port;
        config.maxItems = maxItems;
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

    @Parameter(names = "-hbase", description = "Enable HBase storage", required = false)
    private boolean hbase = false;
    
    @Parameter(names = "-hbaseTimestampTable", description = "HBase timestamp table name", required = false)
    private String hbaseTimestampTable = "OMID_TIMESTAMP";
    
    @Parameter(names = "-hbaseCommitTable", description = "HBase commit table name", required = false)
    private String hbaseCommitTable = "OMID_COMMIT_TABLE";

    @Parameter(names = "-port", description = "Port reserved by the Status Oracle", required = true)
    private int port;

    @Parameter(names = "-metrics", description = "Metrics config", variableArity = true)
    private List<String> metrics = new ArrayList<String>();
    
    @Parameter(names = "-maxItems", description = "Maximum number of items in the TSO (will determine the 'low watermark')")
    private int maxItems = 1000000;

    @Override
    public int processVariableArity(String optionName,
                                    String[] options) {
        return options.length;
    }
    
    public boolean isHBase() {
        return hbase;
    }
    
    public String getHBaseTimestampTable() {
        return hbaseTimestampTable;
    }
    
    public String getHBaseCommitTable() {
        return hbaseCommitTable;
    }

    public int getPort() {
        return port;
    }

    public List<String> getMetrics() {
        return metrics;
    }

    public int getMaxItems() {
        return maxItems;
    }

}
