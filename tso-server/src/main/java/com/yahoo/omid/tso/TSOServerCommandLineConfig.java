/**
 * Copyright 2011-2015 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.tso;

import static com.yahoo.omid.committable.hbase.HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.metrics.CodahaleMetricsProvider.DEFAULT_CODAHALE_METRICS_CONFIG;
import static com.yahoo.omid.timestamp.storage.ZKTimestampStorage.DEFAULT_ZK_CLUSTER;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.DEFAULT_BATCH_PERSIST_TIMEOUT_MS;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.DEFAULT_MAX_BATCH_SIZE;
import static com.yahoo.omid.tso.RequestProcessorImpl.DEFAULT_MAX_ITEMS;
import static com.yahoo.omid.tso.hbase.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

import com.beust.jcommander.IVariableArity;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.yahoo.omid.committable.hbase.HBaseLogin;
import com.yahoo.omid.metrics.MetricsProvider;
import com.yahoo.omid.tsoclient.TSOClient;

/**
 * Holds the configuration parameters of a TSO server instance.
 */
public class TSOServerCommandLineConfig extends JCommander implements IVariableArity {

    // Default network interface prefix where this instance can be running
    static final String LINUX_TSO_NET_IFACE_PREFIX = "eth";
    static final String MAC_TSO_NET_IFACE_PREFIX = "en";

    public enum TimestampStore {
        MEMORY, HBASE, ZK
    };

    public enum CommitTableStore {
        MEMORY, HBASE
    };

    TSOServerCommandLineConfig() {
        this(new String[] {});
    }

    TSOServerCommandLineConfig(String[] args) {
        super();
        addObject(this);
        parse(args);
        setProgramName(TSOServer.class.getName());
    }

    // used for testing
    static public TSOServerCommandLineConfig configFactory(int port, int maxItems) {
        TSOServerCommandLineConfig config = new TSOServerCommandLineConfig();
        config.port = port;
        config.maxItems = maxItems;
        return config;
    }

    static public TSOServerCommandLineConfig parseConfig(String args[]) {
        return new TSOServerCommandLineConfig(args);
    }

    @Parameter(names="-help", description = "Print command options and exit", help = true)
    private boolean help = false;

    @Parameter(names = "-timestampStore", description = "Available stores, MEMORY, HBASE, ZK")
    private TimestampStore timestampStore = TimestampStore.MEMORY;

    @Parameter(names = "-zkCluster", description = "Zookeeper cluster in form: <host>:<port>,<host>,<port>,...)")
    private String zkCluster = DEFAULT_ZK_CLUSTER;

    @Parameter(names = "-commitTableStore", description = "Available stores, MEMORY, HBASE")
    private CommitTableStore commitTableStore = CommitTableStore.MEMORY;

    @Parameter(names = "-hbaseTimestampTable", description = "HBase timestamp table name")
    private String hbaseTimestampTable = TIMESTAMP_TABLE_DEFAULT_NAME;

    @Parameter(names = "-hbaseCommitTable", description = "HBase commit table name")
    private String hbaseCommitTable = COMMIT_TABLE_DEFAULT_NAME;

    @Parameter(names = "-port", description = "Port reserved by the Status Oracle")
    private int port = TSOClient.DEFAULT_TSO_PORT;

    @Parameter(names = "-metricsProvider", description = "Metrics provider: CODAHALE")
    private MetricsProvider.Provider metricsProvider = MetricsProvider.Provider.CODAHALE;

    @Parameter(names = "-metricsConfigs", description = "Metrics config", variableArity = true)
    private List<String> metricsConfigs = new ArrayList<String>(Arrays.asList(DEFAULT_CODAHALE_METRICS_CONFIG));

    @Parameter(names = "-maxItems", description = "Maximum number of items in the TSO (will determine the 'low watermark')")
    private int maxItems = DEFAULT_MAX_ITEMS;

    @Parameter(names = "-maxBatchSize", description = "Maximum size in each persisted batch of commits")
    private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;

    @Parameter(names = "-batchPersistTimeout", description = "Number of milliseconds the persist processer will wait without new input before flushing a batch")
    private int batchPersistTimeoutMS = DEFAULT_BATCH_PERSIST_TIMEOUT_MS;

    // TODO This is probably going to be temporary. So, we should remove it later if not required. Otherwise
    // we should make it private and provide accessors as is done with the other parameters
    @Parameter(names = "-publishHostAndPortInZK", description = "Publishes the host:port of this TSO server in ZK")
    public boolean shouldHostAndPortBePublishedInZK = false;

    @Parameter(names = "-networkIface", description = "Network Interface where TSO is attached to (e.g. eth0, en0...)")
    private String networkIfaceName = getDefaultNetworkIntf();

    @ParametersDelegate
    private HBaseLogin.Config loginFlags = new HBaseLogin.Config();

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

    public TimestampStore getTimestampStore() {
        return timestampStore;
    }

    public String getZKCluster() {
        return zkCluster;
    }

    public CommitTableStore getCommitTableStore() {
        return commitTableStore;
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

    public MetricsProvider.Provider getMetricsProvider() {
        return metricsProvider;
    }

    public List<String> getMetricsConfigs() {
        return metricsConfigs;
    }

    public int getMaxItems() {
        return maxItems;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public int getBatchPersistTimeoutMS() {
        return batchPersistTimeoutMS;
    }

    public HBaseLogin.Config getLoginFlags() { return loginFlags; }

    public String getNetworkIface() {
        return networkIfaceName;
    }

    private static String getDefaultNetworkIntf() {
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                String name = networkInterfaces.nextElement().getDisplayName();
                if (name.startsWith(MAC_TSO_NET_IFACE_PREFIX) || name.startsWith(LINUX_TSO_NET_IFACE_PREFIX)) {
                    return name;
                }
            }
        } catch (SocketException e) {
            throw new IllegalStateException("Can't get network interfaces");
        }
        throw new IllegalArgumentException(String.format("No network '%s*'/'%s*' interfaces found",
                MAC_TSO_NET_IFACE_PREFIX, LINUX_TSO_NET_IFACE_PREFIX));
    }

}