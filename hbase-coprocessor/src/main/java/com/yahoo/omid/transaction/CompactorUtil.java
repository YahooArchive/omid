package com.yahoo.omid.transaction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import com.yahoo.omid.tools.hbase.HBaseLogin;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class CompactorUtil {

    public static void enableOmidCompaction(Configuration conf,
            TableName table, byte[] columnFamily) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        try {
            HTableDescriptor desc = admin.getTableDescriptor(table);
            HColumnDescriptor cfDesc = desc.getFamily(columnFamily);
            cfDesc.setValue(OmidCompactor.OMID_COMPACTABLE_CF_FLAG,
                            Boolean.TRUE.toString());
            admin.modifyColumn(table, cfDesc);
        } finally {
            admin.close();
        }
    }

    public static void disableOmidCompaction(Configuration conf,
            TableName table, byte[] columnFamily) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        try {
            HTableDescriptor desc = admin.getTableDescriptor(table);
            HColumnDescriptor cfDesc = desc.getFamily(columnFamily);
            cfDesc.setValue(OmidCompactor.OMID_COMPACTABLE_CF_FLAG,
                            Boolean.FALSE.toString());
            admin.modifyColumn(table, cfDesc);
        } finally {
            admin.close();
        }
    }

    static class Config {
        @Parameter(names="-table", required=true)
        String table;

        @Parameter(names="-columnFamily", required=false)
        String columnFamily;

        @Parameter(names="-help")
        boolean help = false;

        @Parameter(names="-enable")
        boolean enable = false;

        @Parameter(names="-disable")
        boolean disable = false;

        @ParametersDelegate
        private HBaseLogin.Config loginFlags = new HBaseLogin.Config();

    }

    public static void main(String[] args) throws IOException {
        Config cmdline = new Config();
        JCommander jcommander = new JCommander(cmdline, args);
        if (cmdline.help) {
            jcommander.usage("CompactorUtil");
            System.exit(1);
        }

        HBaseLogin.loginIfNeeded(cmdline.loginFlags);

        Configuration conf = HBaseConfiguration.create();
        if (cmdline.enable) {
            enableOmidCompaction(conf, TableName.valueOf(cmdline.table),
                                 Bytes.toBytes(cmdline.columnFamily));
        } else if (cmdline.disable) {
            disableOmidCompaction(conf, TableName.valueOf(cmdline.table),
                                  Bytes.toBytes(cmdline.columnFamily));
        } else {
            System.err.println("Must specify enable or disable");
        }
    }
}
