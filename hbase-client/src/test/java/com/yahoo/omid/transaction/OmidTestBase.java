package com.yahoo.omid.transaction;

import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.hbase.CommitTableConstants;
import com.yahoo.omid.committable.hbase.CreateTable;
import com.yahoo.omid.tsoclient.TSOClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;

public abstract class OmidTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(OmidTestBase.class);


    static TSOTestBase tso = null;

    static HBaseTestingUtility testutil;
    private static MiniHBaseCluster hbasecluster;
    protected static Configuration hbaseConf;

    protected static final String TEST_TABLE = "test";
    protected static final String TEST_FAMILY = "data";
    static final String TEST_FAMILY2 = "data2";

    private static final TableName TABLE_NAME = TableName.valueOf(TEST_TABLE);

    TSOTestBase getTSO() {
        return tso;
    }

    @BeforeClass
    public void beforeClass() throws Exception {
        LOG.info("Setting up OmidTestBase...");

        // TSO Setup
        tso = new TSOTestBase();
        tso.setupTSO();

        // HBase setup
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 10_000 * 1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);
        hbaseConf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 3);

        File tempFile = File.createTempFile("OmidTest", "");
        tempFile.deleteOnExit();
        hbaseConf.set("hbase.rootdir", tempFile.getAbsolutePath());

        LOG.info("Create hbase");

        // ------------------------------------------------------------------------------------------------------------
        // HBase setup
        // ------------------------------------------------------------------------------------------------------------
        LOG.info("Creating HBase minicluster");
        testutil = new HBaseTestingUtility(hbaseConf);
        hbasecluster = testutil.startMiniCluster(1);

        createTables();

        LOG.info("Setup done");
    }


    private static void createTables() throws IOException {
        HBaseAdmin admin = testutil.getHBaseAdmin();

        HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
        HColumnDescriptor datafam = new HColumnDescriptor(TEST_FAMILY);
        HColumnDescriptor datafam2 = new HColumnDescriptor(TEST_FAMILY2);
        datafam.setMaxVersions(Integer.MAX_VALUE);
        datafam2.setMaxVersions(Integer.MAX_VALUE);
        desc.addFamily(datafam);
        desc.addFamily(datafam2);

        admin.createTable(desc);

        CreateTable.createTable(hbaseConf, CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME, 1);
    }

    protected TransactionManager newTransactionManager() throws Exception {
        return newTransactionManager(tso.getClient());
    }

    protected TransactionManager newTransactionManager(TSOClient tsoClient) throws Exception {
        return HBaseTransactionManager.newBuilder()
            .withConfiguration(hbaseConf)
            .withCommitTableClient(tso.getCommitTable().getClient().get())
            .withTSOClient(tsoClient).build();
    }

    protected TransactionManager newTransactionManager(CommitTable.Client commitTableClient) throws Exception {
        return HBaseTransactionManager.newBuilder()
            .withConfiguration(hbaseConf)
            .withCommitTableClient(commitTableClient)
            .withTSOClient(tso.getClient()).build();
    }

    @AfterClass
    public void afterClass() throws Exception {
        LOG.info("Tearing down OmidTestBase...");
        if (hbasecluster != null) {
            testutil.shutdownMiniCluster();
        }

        tso.teardownTSO();
    }


    @AfterMethod
    public void afterMethod() {
        try {
            LOG.info("tearing Down");
            HBaseAdmin admin = testutil.getHBaseAdmin();
            truncateTable(admin, TableName.valueOf(TEST_TABLE));
            truncateTable(admin, TableName.valueOf(CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME));

        } catch (Exception e) {
            LOG.error("Error tearing down", e);
        }
    }

    private void truncateTable(HBaseAdmin admin, TableName tableName) throws IOException {
        admin.disableTable(TEST_TABLE);
        admin.truncateTable(tableName, true);
        admin.enableTable(TEST_TABLE);
    }

    static boolean verifyValue(byte[] tableName, byte[] row,
                               byte[] fam, byte[] col, byte[] value) {

        try (HTable table = new HTable(hbaseConf, tableName)) {
            Get g = new Get(row).setMaxVersions(1);
            Result r = table.get(g);
            Cell cell = r.getColumnLatestCell(fam, col);

            if (LOG.isTraceEnabled()) {
                LOG.trace("Value for " + Bytes.toString(tableName) + ":"
                          + Bytes.toString(row) + ":" + Bytes.toString(fam)
                          + Bytes.toString(col) + "=>" + Bytes.toString(CellUtil.cloneValue(cell))
                          + " (" + Bytes.toString(value) + " expected)");
            }

            return Bytes.equals(CellUtil.cloneValue(cell), value);
        } catch (IOException e) {
            LOG.error("Error reading row " + Bytes.toString(tableName) + ":"
                      + Bytes.toString(row) + ":" + Bytes.toString(fam)
                      + Bytes.toString(col), e);
            return false;
        }
    }
}
