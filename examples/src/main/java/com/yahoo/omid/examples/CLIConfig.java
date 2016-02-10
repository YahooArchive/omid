package com.yahoo.omid.examples;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.yahoo.omid.committable.hbase.CommitTableConstants;
import com.yahoo.omid.tools.hbase.HBaseLogin;
import com.yahoo.omid.tsoclient.TSOClient;

/**
 * @author katkovi@
 */
class CLIConfig extends HBaseLogin.Config {

    @Parameter(names = "-tsoHost", description = "TSO host name")
    String tsoHost = TSOClient.DEFAULT_TSO_HOST;

    @Parameter(names = "-tsoPort", description = "TSO host port")
    int tsoPort = TSOClient.DEFAULT_TSO_PORT;
    @Parameter(names = "-hbaseConfig", description = "Location of hbase-site.xml, if not specified loads from classpath")
    String hbaseConfig;
    @Parameter(names = "-hadoopConfig", description = "Location of core-site.xml, if not specified loads from classpath")
    String hadoopConfig;
    @Parameter(names = "-commitTableName", description = "CommitTable name")
    String commitTableName = CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME;
    @Parameter(names = "-userTableName", description = "User table name")
    String userTableName = "EXAMPLE_TABLE";
    @Parameter(names = "-cf", description = "User table column family")
    String cfName = "MY_CF";

    private CLIConfig() {
    }

    static CLIConfig parse(String[] args) {
        CLIConfig commandLineConfig = new CLIConfig();

        JCommander commandLine = new JCommander(commandLineConfig);
        try {
            commandLine.parse(args);
        } catch (ParameterException ex) {
            commandLine.usage();
            throw new IllegalArgumentException(ex.getMessage());
        }

        return commandLineConfig;
    }
}
