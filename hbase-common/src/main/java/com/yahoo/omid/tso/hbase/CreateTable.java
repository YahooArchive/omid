package com.yahoo.omid.tso.hbase;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.yahoo.omid.committable.hbase.HBaseLogin;
import com.yahoo.omid.timestamp.storage.HBaseTimestampStorage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

//TODO: IK: Move to some other java package? tools?
public class CreateTable {

    private static final Logger LOG = LoggerFactory.getLogger(CreateTable.class);

    private static class Config {

        @Parameter(names = "-tableName", description = "Name of the timestamp storage table in HBase", required = false)
        String table = HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;

        @ParametersDelegate
        HBaseLogin.Config loginFlags = new HBaseLogin.Config();
    }

    public static void main(String[] args) throws IOException {

        Config config = new Config();
        new JCommander(config, args);

        HBaseLogin.loginIfNeeded(config.loginFlags);
        createTable(HBaseConfiguration.create(), config.table);
    }

    private static void createTable(Configuration hbaseConf, String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        if (!admin.tableExists(tableName)) {
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor datafam = new HColumnDescriptor(HBaseTimestampStorage.TSO_FAMILY);
            datafam.setMaxVersions(3);
            desc.addFamily(datafam);
            admin.createTable(desc);
        }

        if (admin.isTableDisabled(tableName)) {
            admin.enableTable(tableName);
        }
        admin.close();
        LOG.info("Table {} created successfully", tableName);
    }

}
