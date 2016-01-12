package com.yahoo.omid.committable.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class TestTableCreation {

    private static final Logger LOG = LoggerFactory.getLogger(TestTableCreation.class);

    private static HBaseTestingUtility testUtil;
    private static MiniHBaseCluster hbaseCluster;
    protected static Configuration hbaseConf;

    @BeforeClass
    public static void setUpClass() throws Exception {
        // HBase setup
        hbaseConf = HBaseConfiguration.create();

        LOG.info("Starting hbase");
        testUtil = new HBaseTestingUtility(hbaseConf);
        hbaseCluster = testUtil.startMiniCluster(1);

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (hbaseCluster != null) {
            testUtil.shutdownMiniCluster();
        }
    }

    @Test
    public void testCreateTable() throws Throwable {
        HBaseAdmin admin = testUtil.getHBaseAdmin();

        String tableName = "Table_with_single_region";
        CreateTable.createTable(hbaseConf, tableName, 1);
        int numRegions = admin.getTableRegions(Bytes.toBytes(tableName)).size();
        assertEquals("Should have only 1 region", 1, numRegions);

        tableName = "Table_with_multiple_region";
        CreateTable.createTable(hbaseConf, tableName, 60);
        numRegions = admin.getTableRegions(Bytes.toBytes(tableName)).size();
        assertEquals("Should have 60 regions", 60, numRegions);
    }

}
