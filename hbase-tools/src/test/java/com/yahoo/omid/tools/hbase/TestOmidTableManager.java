package com.yahoo.omid.tools.hbase;

import com.yahoo.omid.committable.hbase.HBaseCommitTableConfig;
import com.yahoo.omid.timestamp.storage.HBaseTimestampStorageConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.yahoo.omid.tools.hbase.OmidTableManager.COMMIT_TABLE_COMMAND_NAME;
import static com.yahoo.omid.tools.hbase.OmidTableManager.TIMESTAMP_TABLE_COMMAND_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestOmidTableManager {
    private HBaseTestingUtility hBaseTestUtil;
    private Configuration hbaseConf;
    private HBaseAdmin hBaseAdmin;

    @BeforeClass
    public void setUpClass() throws Exception {
        // HBase setup
        hbaseConf = HBaseConfiguration.create();

        hBaseTestUtil = new HBaseTestingUtility(hbaseConf);
        hBaseTestUtil.startMiniCluster(1);

        hBaseAdmin = hBaseTestUtil.getHBaseAdmin();
    }

    @AfterClass
    public void tearDownClass() throws Exception {

        hBaseAdmin.close();

        hBaseTestUtil.shutdownMiniCluster();

    }

    @Test(timeOut = 20_000)
    public void testCreateDefaultTimestampTableSucceeds() throws Throwable {

        String[] args = new String[]{TIMESTAMP_TABLE_COMMAND_NAME};

        OmidTableManager omidTableManager = new OmidTableManager(args);
        omidTableManager.executeActionsOnHBase(hbaseConf);

        TableName tableName = TableName.valueOf(HBaseTimestampStorageConfig.DEFAULT_TIMESTAMP_STORAGE_TABLE_NAME);

        assertTrue(hBaseAdmin.tableExists(tableName));
        int numRegions = hBaseAdmin.getTableRegions(tableName).size();
        assertEquals(numRegions, 1, "Should have only 1 region");

    }

    @Test(timeOut = 20_000)
    public void testCreateDefaultCommitTableSucceeds() throws Throwable {

        String[] args = new String[]{COMMIT_TABLE_COMMAND_NAME};

        OmidTableManager omidTableManager = new OmidTableManager(args);
        omidTableManager.executeActionsOnHBase(hbaseConf);

        TableName tableName = TableName.valueOf(HBaseCommitTableConfig.DEFAULT_COMMIT_TABLE_NAME);

        assertTrue(hBaseAdmin.tableExists(tableName));
        int numRegions = hBaseAdmin.getTableRegions(tableName).size();
        assertEquals(numRegions, 16, "Should have 16 regions");

    }

    @Test(timeOut = 20_000)
    public void testCreateCustomCommitTableSucceeds() throws Throwable {

        String[] args = new String[]{COMMIT_TABLE_COMMAND_NAME, "-tableName", "my-commit-table", "-numRegions", "1"};

        OmidTableManager omidTableManager = new OmidTableManager(args);
        omidTableManager.executeActionsOnHBase(hbaseConf);

        TableName tableName = TableName.valueOf("my-commit-table");

        assertTrue(hBaseAdmin.tableExists(tableName));
        int numRegions = hBaseAdmin.getTableRegions(tableName).size();
        assertEquals(numRegions, 1, "Should have only 1 regions");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, timeOut = 20_000)
    public void testExceptionIsThrownWhenSpecifyingAWrongCommand() throws Throwable {

        String[] args = new String[]{"non-recognized-command"};

        OmidTableManager omidTableManager = new OmidTableManager(args);
        omidTableManager.executeActionsOnHBase(hbaseConf);

    }

}
