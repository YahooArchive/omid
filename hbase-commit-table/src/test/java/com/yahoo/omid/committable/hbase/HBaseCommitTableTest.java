package com.yahoo.omid.committable.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.concurrent.Future;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.omid.committable.CommitTable.Client;
import com.yahoo.omid.committable.CommitTable.Writer;

public class HBaseCommitTableTest {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseCommitTableTest.class);

    private static final String TEST_TABLE = "TEST";

    private static HBaseTestingUtility testutil;
    private static MiniHBaseCluster hbasecluster;
    protected static Configuration hbaseConf;
    private static AggregationClient aggregationClient;

    @BeforeClass
    public static void setUpClass() throws Exception {
        // HBase setup
        hbaseConf = HBaseConfiguration.create();

        LOG.info("Create hbase");
        testutil = new HBaseTestingUtility(hbaseConf);
        hbasecluster = testutil.startMiniCluster(1);
        aggregationClient = new AggregationClient(hbaseConf);

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
            HColumnDescriptor datafam = new HColumnDescriptor(HBaseCommitTable.COMMIT_TABLE_FAMILY);
            datafam.setMaxVersions(Integer.MAX_VALUE);
            desc.addFamily(datafam);
            desc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
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
    public void testCreateTable() throws Throwable {
        HBaseAdmin admin = testutil.getHBaseAdmin();

        String tableName = "Table_with_single_region";
        CreateTable.createTable(hbaseConf, tableName, 1);
        int numRegions = admin.getTableRegions(Bytes.toBytes(tableName)).size();
        assertEquals("Should have only 1 region", 1, numRegions);

        tableName = "Table_with_multiple_region";
        CreateTable.createTable(hbaseConf, tableName, 60);
        numRegions = admin.getTableRegions(Bytes.toBytes(tableName)).size();
        assertEquals("Should have 60 regions", 60, numRegions);
    }

    @Test
    public void testBasicBehaviour() throws Throwable {
        HBaseCommitTable commitTable = new HBaseCommitTable(hbaseConf, TEST_TABLE);

        ListenableFuture<Writer> futureWriter = commitTable.getWriter();
        Writer writer = futureWriter.get();
        ListenableFuture<Client> futureClient = commitTable.getClient();
        Client client = futureClient.get();

        // Test that the first time the table is empty
        assertEquals("Rows should be 0!", 0, rowCount());

        // Test the successful creation of 1000 txs in the table
        for (int i = 0; i < 1000; i++) {
            writer.addCommittedTransaction(i, i + 1);
        }
        writer.flush().get();
        assertEquals("Rows should be 1000!", 1000, rowCount());

        // Test the we get the right commit timestamps for each previously inserted tx
        for (long i = 0; i < 1000; i++) {
            ListenableFuture<Optional<Long>> ctf = client.getCommitTimestamp(i);
            Optional<Long> optional = ctf.get();
            Long ct = optional.get();
            assertEquals("Commit timestamp should be " + (i + 1), (i + 1), (long) ct);
        }
        assertEquals("Rows should be 1000!", 1000, rowCount());

        // Test the successful deletion of the 1000 txs
        Future<Void> f = null;
        for (long i = 0; i < 1000; i++) {
            f = client.completeTransaction(i);
        }
        f.get();
        assertEquals("Rows should be 0!", 0, rowCount());

        // Test we don't get a commit timestamp for a non-existent transaction id in the table
        ListenableFuture<Optional<Long>> ctf = client.getCommitTimestamp(0);
        Optional<Long> optional = ctf.get();
        assertFalse("Commit timestamp should not be present", optional.isPresent());

    }

    private static long rowCount() throws Throwable {
        Scan scan = new Scan();
        scan.addFamily(HBaseCommitTable.COMMIT_TABLE_FAMILY);
        return aggregationClient.rowCount(Bytes.toBytes(TEST_TABLE), null, scan);
    }

}
