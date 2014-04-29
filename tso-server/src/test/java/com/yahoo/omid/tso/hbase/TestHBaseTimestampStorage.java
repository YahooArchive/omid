package com.yahoo.omid.tso.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHBaseTimestampStorage {

    private static final Logger LOG = LoggerFactory.getLogger(TestHBaseTimestampStorage.class);

    private static final String TEST_TABLE = "TEST";

    private static HBaseTestingUtility testutil;
    private static MiniHBaseCluster hbasecluster;
    protected static Configuration hbaseConf;


    @BeforeClass
    public static void setUpClass() throws Exception {
        // HBase setup
        hbaseConf = HBaseConfiguration.create();

        LOG.info("Create hbase");
        testutil = new HBaseTestingUtility(hbaseConf);
        hbasecluster = testutil.startMiniCluster(1);

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (hbasecluster != null) {
            testutil.shutdownMiniCluster();
        }
    }

    @Before
    public void setUp() throws Exception {
        HBaseAdmin admin = testutil.getHBaseAdmin();

        if (!admin.tableExists(TEST_TABLE)) {
            HTableDescriptor desc = new HTableDescriptor(TEST_TABLE);
            HColumnDescriptor datafam = new HColumnDescriptor(HBaseTimestampStorage.TSO_FAMILY);
            datafam.setMaxVersions(Integer.MAX_VALUE);
            desc.addFamily(datafam);

            admin.createTable(desc);
        }

        if (admin.isTableDisabled(TEST_TABLE)) {
            admin.enableTable(TEST_TABLE);
        }
        HTableDescriptor[] tables = admin.listTables();
        for (HTableDescriptor t : tables) {
            LOG.info(t.getNameAsString());
        }
    }

    @After
    public void tearDown() {
        try {
            LOG.info("tearing Down");
            HBaseAdmin admin = testutil.getHBaseAdmin();
            admin.disableTable(TEST_TABLE);
            admin.deleteTable(TEST_TABLE);

        } catch (Exception e) {
            LOG.error("Error tearing down", e);
        }
    }

    @Test
    public void testHBaseTimestampStorage() throws Exception {

        final long INITIAL_TS_VALUE = 0;
        HTable table = new HTable(hbaseConf, TEST_TABLE);
        HBaseTimestampStorage tsStorage = new HBaseTimestampStorage(table);

        // Test that the first time we get the timestamp is the initial value
        assertEquals("Initial value should be " + INITIAL_TS_VALUE, INITIAL_TS_VALUE, tsStorage.getMaxTimestamp());

        // Test that updating the timestamp succeeds when passing the initial value as the previous one
        long newTimestamp = 1;
        tsStorage.updateMaxTimestamp(INITIAL_TS_VALUE, newTimestamp);

        // Test setting a new timestamp fails (exception is thrown) when passing a wrong previous max timestamp
        long wrongTimestamp = 20;
        try {
            tsStorage.updateMaxTimestamp(wrongTimestamp, newTimestamp);
            fail("Shouldn't update");
        } catch (IOException e) {
            // Correct behavior
        }
        assertEquals("Value should be still " + newTimestamp, newTimestamp, tsStorage.getMaxTimestamp());

        // Test we can set a new timestamp when passing the right previous max timestamp
        long veryNewTimestamp = 40;
        tsStorage.updateMaxTimestamp(newTimestamp, veryNewTimestamp);
        assertEquals("Value should be " + veryNewTimestamp, veryNewTimestamp, tsStorage.getMaxTimestamp());
    }

}
