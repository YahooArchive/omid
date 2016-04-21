/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.tools.hbase;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.apache.omid.HBaseShims;
import org.apache.omid.committable.hbase.HBaseCommitTableConfig;
import org.apache.omid.committable.hbase.KeyGenerator;
import org.apache.omid.committable.hbase.KeyGeneratorImplementations;
import org.apache.omid.committable.hbase.RegionSplitter;
import org.apache.omid.timestamp.storage.HBaseTimestampStorageConfig;
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

/**
 * Helper class to create required HBase tables by Omid
 */
public class OmidTableManager {

    private static final Logger LOG = LoggerFactory.getLogger(OmidTableManager.class);

    public static final String COMMIT_TABLE_COMMAND_NAME = "commit-table";
    static final String TIMESTAMP_TABLE_COMMAND_NAME = "timestamp-table";

    private static final byte[][] commitTableFamilies = new byte[][]{
            HBaseCommitTableConfig.DEFAULT_COMMIT_TABLE_CF_NAME.getBytes(),
            HBaseCommitTableConfig.DEFAULT_COMMIT_TABLE_LWM_CF_NAME.getBytes()};
    private static final byte[][] timestampTableFamilies = new byte[][]{
            HBaseTimestampStorageConfig.DEFAULT_TIMESTAMP_STORAGE_CF_NAME.getBytes()};

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
            byte[][] tableFamilies;
            byte[][] splitKeys = new byte[0][0];
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

    private static byte[][] splitInUniformRegions(Configuration hBaseConf, int numRegions) throws IOException {

        KeyGenerator keyGen = KeyGeneratorImplementations.defaultKeyGenerator();
        RegionSplitter.SplitAlgorithm algo =
                RegionSplitter.newSplitAlgoInstance(hBaseConf, RegionSplitter.UniformSplit.class.getName());
        algo.setFirstRow(algo.rowToStr(keyGen.startTimestampToKey(0)));
        algo.setLastRow(algo.rowToStr(keyGen.startTimestampToKey(Long.MAX_VALUE)));

        // Return the split keys
        return algo.split(numRegions);

    }

    private static void createTable(HBaseAdmin admin, String tableName, byte[][] families, byte[][] splitKeys,
                                    int maxVersions)
            throws IOException {

        LOG.info("About to create Table named {} with {} splits", tableName, splitKeys.length);

        if (admin.tableExists(tableName)) {
            LOG.error("Table {} already exists. Table creation cancelled", tableName);
            return;
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        for (byte[] family : families) {
            HColumnDescriptor colDescriptor = new HColumnDescriptor(family);
            colDescriptor.setMaxVersions(maxVersions);
            HBaseShims.addFamilyToHTableDescriptor(tableDescriptor, colDescriptor);
            LOG.info("\tAdding Family {}", colDescriptor);
        }

        admin.createTable(tableDescriptor, splitKeys);

        LOG.info("Table {} created. Regions: {}", tableName, admin.getTableRegions(Bytes.toBytes(tableName)).size());

    }

    // Configuration-related classes

    static class MainConfig {

        @ParametersDelegate
        SecureHBaseConfig loginFlags = new SecureHBaseConfig();

    }

    @Parameters(commandDescription = "Specifies configuration for the Commit Table")
    static class CommitTableCommand {

        @Parameter(names = "-tableName", description = "Table name where to stores the commits", required = false)
        String tableName = HBaseCommitTableConfig.DEFAULT_COMMIT_TABLE_NAME;

        @Parameter(names = "-numRegions", description = "Number of splits (to pre-split tableName)", required = false,
                   validateWith = IntegerGreaterThanZero.class)
        int numRegions = 16;

    }

    @Parameters(commandDescription = "Specifies configuration for the Timestamp Table")
    static class TimestampTableCommand {

        @Parameter(names = "-tableName", description = "Table name where to store timestamps")
        String tableName = HBaseTimestampStorageConfig.DEFAULT_TIMESTAMP_STORAGE_TABLE_NAME;

    }

    public static class IntegerGreaterThanZero implements IParameterValidator {

        public void validate(String name, String value) throws ParameterException {
            int n = Integer.parseInt(value);
            if (n <= 0) {
                throw new ParameterException("Parameter " + name + " should be > 0 (found " + value + ")");
            }
        }

    }

}
