package com.yahoo.omid.tso;

import static com.yahoo.omid.committable.hbase.HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.metrics.CodahaleMetricsProvider.DEFAULT_CODAHALE_METRICS_CONFIG;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.DEFAULT_BATCH_PERSIST_TIMEOUT_MS;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.DEFAULT_MAX_BATCH_SIZE;
import static com.yahoo.omid.tso.RequestProcessorImpl.DEFAULT_MAX_ITEMS;
import static com.yahoo.omid.tso.hbase.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;

import java.util.ArrayList;
import java.util.Arrays;
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

    @Parameter(names = "-hbase", description = "Enable HBase storage")
    private boolean hbase = false;

    @Parameter(names = "-hbaseTimestampTable", description = "HBase timestamp table name")
    private String hbaseTimestampTable = TIMESTAMP_TABLE_DEFAULT_NAME;

    @Parameter(names = "-hbaseCommitTable", description = "HBase commit table name")
    private String hbaseCommitTable = COMMIT_TABLE_DEFAULT_NAME;

    @Parameter(names = "-port", description = "Port reserved by the Status Oracle")
    private int port = TSOClient.DEFAULT_TSO_PORT;

    @Parameter(names = "-metricsProvider", description = "Metrics provider: CODAHALE | YMON")
    private MetricsProvider.Provider metricsProvider = MetricsProvider.Provider.CODAHALE;

    @Parameter(names = "-metricsConfigs", description = "Metrics config", variableArity = true)
    private List<String> metricsConfigs = new ArrayList<String>(Arrays.asList(DEFAULT_CODAHALE_METRICS_CONFIG));

    @Parameter(names = "-maxItems", description = "Maximum number of items in the TSO (will determine the 'low watermark')")
    private int maxItems = DEFAULT_MAX_ITEMS;

    @Parameter(names = "-maxBatchSize", description = "Maximum size in each persisted batch of commits")
    private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;

    @Parameter(names = "-batchPersistTimeout", description = "Number of milliseconds the persist processer will wait without new input before flushing a batch")
    private int batchPersistTimeoutMS = DEFAULT_BATCH_PERSIST_TIMEOUT_MS;

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
}
