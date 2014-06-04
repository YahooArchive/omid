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

import static com.yahoo.omid.tso.hbase.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.IVariableArity;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.tsoclient.TSOClient;

/**
 * Holds the configuration parameters of a TSO server instance.
 * 
 */
public class TSOServerConfig extends JCommander implements IVariableArity {

    TSOServerConfig() {
        this(new String[] {});
    }

    TSOServerConfig(String[] args) {
        super();
        addObject(this);
        parse(args);
        setProgramName(TSOServer.class.getName());
    }

    // used for testing
    static public TSOServerConfig configFactory(int port, int maxItems) {
        TSOServerConfig config = new TSOServerConfig();
        config.port = port;
        config.maxItems = maxItems;
        return config;
    }

    static public TSOServerConfig parseConfig(String args[]) {
        return new TSOServerConfig(args);
    }

    @Parameter(names="-help", description = "Print command options and exit", help = true)
    private boolean help = false;
    
    @Parameter(names = "-hbase", description = "Enable HBase storage")
    private boolean hbase = false;

    @Parameter(names = "-hbaseTimestampTable", description = "HBase timestamp table name")
    private String hbaseTimestampTable = TIMESTAMP_TABLE_DEFAULT_NAME;
    
    @Parameter(names = "-hbaseCommitTable", description = "HBase commit table name")
    private String hbaseCommitTable = HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME;

    @Parameter(names = "-port", description = "Port reserved by the Status Oracle")
    private int port = TSOClient.DEFAULT_TSO_PORT;

    @Parameter(names = "-metrics", description = "Metrics config", variableArity = true)
    private List<String> metrics = new ArrayList<String>();
    
    @Parameter(names = "-maxItems", description = "Maximum number of items in the TSO (will determine the 'low watermark')")
    private int maxItems = 1000000;

    @Parameter(names = "-maxBatchSize", description = "Maximum size in each persisted batch of commits")
    private int maxBatchSize = 10000;

    @Override
    public int processVariableArity(String optionName,
                                    String[] options) {
        int i = 0;
        for (String o: options) {
            if (o.startsWith("-")) {
                return i;
            }
            i++;
        }
        return i;
    }
    
    public boolean hasHelpFlag() {
        return help;
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

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

}
