package com.yahoo.omid.committable.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import static com.yahoo.omid.committable.hbase.CommitTableConstants.COMMIT_TABLE_FAMILY;
import static com.yahoo.omid.committable.hbase.CommitTableConstants.LOW_WATERMARK_FAMILY;

/**
 * TODO: Francisco: Create a single helper to create HBase tables and move to some other java package (e.g. tools)
 *   This is because there is another class in the tso.hbase package that does the same for the timestamp table
 */
public class CreateTable {

    private static final Logger LOG = LoggerFactory.getLogger(CreateTable.class);

    static class Config {

        @Parameter(names = "-tableName", description = "Name of the commit table in HBase", required = false)
        String table = CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME;

        @Parameter(names = "-numSplits", description = "Number of splits (to pre-split table)", required = false)
        int numSplits = 1;

        @ParametersDelegate
        HBaseLogin.Config loginFlags = new HBaseLogin.Config();
    }

    public static void main(String[] args) throws IOException {
        Config config = new Config();
        new JCommander(config, args);
        HBaseLogin.loginIfNeeded(config.loginFlags);
        createTable(HBaseConfiguration.create(), config.table, config.numSplits);
    }

    public static void createTable(Configuration hbaseConf, String tableName, int numSplits) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        if (!admin.tableExists(tableName)) {
            KeyGenerator keyGen = KeyGeneratorImplementations.defaultKeyGenerator();

            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor datafam = new HColumnDescriptor(COMMIT_TABLE_FAMILY);
            datafam.setMaxVersions(1);
            desc.addFamily(datafam);
            HColumnDescriptor lowWatermarkFam = new HColumnDescriptor(LOW_WATERMARK_FAMILY);
            lowWatermarkFam.setMaxVersions(1);
            desc.addFamily(lowWatermarkFam);
            if (numSplits > 1) {
                RegionSplitter.SplitAlgorithm algo = RegionSplitter.newSplitAlgoInstance(hbaseConf,
                        RegionSplitter.UniformSplit.class.getName());
                algo.setFirstRow(algo.rowToStr(keyGen.startTimestampToKey(0)));
                algo.setLastRow(algo.rowToStr(keyGen.startTimestampToKey(Long.MAX_VALUE)));
                admin.createTable(desc, algo.split(numSplits));
            } else {
                admin.createTable(desc);
            }

            LOG.info("Created {} table with {} regions",
                    tableName, admin.getTableRegions(Bytes.toBytes(tableName)).size());
        }

        if (admin.isTableDisabled(tableName)) {
            admin.enableTable(tableName);
        }
        admin.close();
        LOG.info("Table {} created successfully", tableName);
    }

}
