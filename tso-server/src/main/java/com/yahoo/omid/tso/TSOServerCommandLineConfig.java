package com.yahoo.omid.tso;

import com.beust.jcommander.IVariableArity;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.yahoo.omid.committable.hbase.HBaseLogin;
import com.yahoo.omid.tsoclient.TSOClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.yahoo.omid.committable.hbase.HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.metrics.CodahaleMetricsProvider.DEFAULT_CODAHALE_METRICS_CONFIG;
import static com.yahoo.omid.timestamp.storage.ZKTimestampStorage.DEFAULT_ZK_CLUSTER;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.DEFAULT_BATCH_PERSIST_TIMEOUT_MS;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.DEFAULT_MAX_BATCH_SIZE;
import static com.yahoo.omid.tso.RequestProcessorImpl.DEFAULT_MAX_ITEMS;
import static com.yahoo.omid.tso.hbase.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;

/**
 * Holds the configuration parameters of a TSO server instance.
 */
public class TSOServerCommandLineConfig extends JCommander implements IVariableArity {

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

    @Parameter(names = "-metricsProviderModule", description = "Guice metrics provider config, e.g. com.yahoo.omid.metrics.CodahaleModule")
    private String metricsProviderModule = "com.yahoo.omid.metrics.CodahaleModule";

    @Parameter(names = "-metricsConfigs", description = "Metrics config", variableArity = true)
    private List<String> metricsConfigs = new ArrayList<>(Arrays.asList(DEFAULT_CODAHALE_METRICS_CONFIG));

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
    private String networkIfaceName = TSOServer.getDefaultNetworkIntf();

    @Parameter(names = "-leasePeriodInMs", description = "Lease period for high availability in ms")
    private long leasePeriodInMs = TSOServer.DEFAULT_LEASE_PERIOD_IN_MSECS;

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

    public void setZKCluster(String zkCluster) {
        this.zkCluster = zkCluster;
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

    public String getMetricsProviderModule() {
        return metricsProviderModule;
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

    public void setLeasePeriodInMs(long leasePeriodInMs) {
        this.leasePeriodInMs = leasePeriodInMs;
    }

    public long getLeasePeriodInMs() {
        return leasePeriodInMs;
    }

}
