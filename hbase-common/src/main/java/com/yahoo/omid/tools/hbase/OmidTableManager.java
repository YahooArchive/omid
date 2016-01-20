package com.yahoo.omid.tools.hbase;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.yahoo.omid.committable.hbase.KeyGenerator;
import com.yahoo.omid.committable.hbase.KeyGeneratorImplementations;
import com.yahoo.omid.committable.hbase.RegionSplitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.yahoo.omid.committable.hbase.CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.committable.hbase.CommitTableConstants.COMMIT_TABLE_FAMILY;
import static com.yahoo.omid.committable.hbase.CommitTableConstants.LOW_WATERMARK_FAMILY;
import static com.yahoo.omid.timestamp.storage.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.timestamp.storage.HBaseTimestampStorage.TSO_FAMILY;

/**
 * Helper class to create required HBase tables by Omid
 */
public class OmidTableManager {

    private static final Logger LOG = LoggerFactory.getLogger(OmidTableManager.class);

    public static final String COMMIT_TABLE_COMMAND_NAME = "commit-table";
    public static final String TIMESTAMP_TABLE_COMMAND_NAME = "timestamp-table";

    private static final byte[][] commitTableFamilies = new byte[][] { COMMIT_TABLE_FAMILY, LOW_WATERMARK_FAMILY };
    private static final byte[][] timestampTableFamilies = new byte[][] { TSO_FAMILY };

    private JCommander commandLine;
    private MainConfig mainConfig = new MainConfig();
    private CommitTableCommand commitTableCommand = new CommitTableCommand();
    private TimestampTableCommand timestampTableCommand = new TimestampTableCommand();

    public OmidTableManager(String... args) {
        commandLine = new JCommander(mainConfig);
        commandLine.addCommand(COMMIT_TABLE_COMMAND_NAME, commitTableCommand);
        commandLine.addCommand(TIMESTAMP_TABLE_COMMAND_NAME, timestampTableCommand);
        try {
            commandLine.parse(args);
        } catch (ParameterException ex) {
            commandLine.usage();
            throw new IllegalArgumentException(ex.getMessage());
        }
    }

    public void executeActionsOnHBase(Configuration hbaseConf) throws IOException {

        HBaseLogin.loginIfNeeded(mainConfig.loginFlags);

        try (HBaseAdmin hBaseAdmin = new HBaseAdmin(hbaseConf)) {
            byte [][] tableFamilies;
            byte [][] splitKeys = new byte[0][0];
            String tableName;

            LOG.info("----------------------------------------------------------------------------------------------");
            switch (commandLine.getParsedCommand()) {
                case COMMIT_TABLE_COMMAND_NAME:
                    LOG.info("Performing actions related to COMMIT TABLE");
                    tableName = commitTableCommand.tableName;
                    tableFamilies = commitTableFamilies;
                    if (commitTableCommand.numRegions > 1) {
                        splitKeys = splitInUniformRegions(hbaseConf, commitTableCommand.numRegions);
                    }
                    break;
                case TIMESTAMP_TABLE_COMMAND_NAME:
                    LOG.info("Performing actions related to TIMESTAMP TABLE");
                    tableName = timestampTableCommand.tableName;
                    tableFamilies = timestampTableFamilies;
                    break;
                default:
                    LOG.error("Unknown command: {}", commandLine.getParsedCommand());
                    commandLine.usage();
                    return;
            }

            createTable(hBaseAdmin, tableName, tableFamilies, splitKeys, 1);
            LOG.info("----------------------------------------------------------------------------------------------");

        }
    }

    public static void main(String... args) throws Exception {

        OmidTableManager tableManager = new OmidTableManager(args);
        tableManager.executeActionsOnHBase(HBaseConfiguration.create());

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods and classes
    // ----------------------------------------------------------------------------------------------------------------

    static byte[][] splitInUniformRegions(Configuration hBaseConf, int numRegions) throws IOException {

        KeyGenerator keyGen = KeyGeneratorImplementations.defaultKeyGenerator();
        RegionSplitter.SplitAlgorithm algo = RegionSplitter.newSplitAlgoInstance(hBaseConf,
                RegionSplitter.UniformSplit.class.getName());
        algo.setFirstRow(algo.rowToStr(keyGen.startTimestampToKey(0)));
        algo.setLastRow(algo.rowToStr(keyGen.startTimestampToKey(Long.MAX_VALUE)));

        // Return the split keys
        return algo.split(numRegions);

    }

    static void createTable(HBaseAdmin admin, String tableName, byte[][] families, byte[][] splitKeys, int maxVersions)
            throws IOException {

        LOG.info("About to create Table named {} with {} splits", tableName, splitKeys.length);

        if (admin.tableExists(tableName)) {
            LOG.error("Table {} already exists. Table creation cancelled", tableName);
            return;
        }

        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

        for(int familyIdx = 0; familyIdx < families.length; ++familyIdx) {
            byte[] family = families[familyIdx];
            HColumnDescriptor colDescriptor = new HColumnDescriptor(family);
            colDescriptor.setMaxVersions(maxVersions);
            desc.addFamily(colDescriptor);
            LOG.info("\tAdding Family {}", colDescriptor);
        }

        admin.createTable(desc, splitKeys);

        LOG.info("Table {} created. Regions: {}", tableName, admin.getTableRegions(Bytes.toBytes(tableName)).size());

    }

    // Configuration-related classes

    static class MainConfig {

        @ParametersDelegate
        HBaseLogin.Config loginFlags = new HBaseLogin.Config();

    }

    @Parameters(commandDescription = "Specifies configuration for the Commit Table")
    static class CommitTableCommand {

        @Parameter(names = "-tableName", description = "Table name where to stores the commits", required = false)
        String tableName = COMMIT_TABLE_DEFAULT_NAME;

        @Parameter(names = "-numRegions", description = "Number of splits (to pre-split tableName)", required = false,
                   validateWith = IntegerGreaterThanZero.class)
        int numRegions = 16;

    }

    @Parameters(commandDescription = "Specifies configuration for the Timestamp Table")
    static class TimestampTableCommand {

        @Parameter(names = "-tableName", description = "Table name where to store timestamps", required = false)
        String tableName = TIMESTAMP_TABLE_DEFAULT_NAME;

    }

    public static class IntegerGreaterThanZero implements IParameterValidator {

        public void validate(String name, String value) throws ParameterException {
            int n = Integer.parseInt(value);
            if (n <= 0) {
                throw new ParameterException("Parameter " + name + " should be > 0 (found " + value +")");
            }
        }

    }

}
