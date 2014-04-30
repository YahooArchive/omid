package com.yahoo.omid.committable.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class CreateTable {

    private static final Logger LOG = LoggerFactory.getLogger(CreateTable.class);

    static class Config {

        @Parameter(names = "-tableName", description = "Name of the commit table in HBase", required = false)
        String table = HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME;

    }

    public static void main(String[] args) throws IOException {

        Config config = new Config();
        new JCommander(config, args);
        createTable(HBaseConfiguration.create(), config.table);

    }

    public static void createTable(Configuration hbaseConf, String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        if (!admin.tableExists(tableName)) {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            HColumnDescriptor datafam = new HColumnDescriptor(HBaseCommitTable.COMMIT_TABLE_FAMILY);
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
