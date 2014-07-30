package com.yahoo.omid.transaction;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.yahoo.omid.transaction.HBaseTransactionManager;
import com.yahoo.omid.transaction.TransactionManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.yahoo.omid.committable.hbase.CreateTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.tso.TSOTestBase;

public class OmidTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(OmidTestBase.class);

    private static TSOTestBase tso = null;   
    protected static HBaseTestingUtility testutil;
    private static MiniHBaseCluster hbasecluster;
    protected static Configuration hbaseConf;

    protected static final String TEST_TABLE = "test";
    protected static final String TEST_FAMILY = "data";
    protected static final String TEST_FAMILY2 = "data2";

    protected static final TableName TABLE_NAME = TableName.valueOf(TEST_TABLE);

    protected TSOTestBase getTSO() {
        return tso;
    }

    @BeforeClass
    public static void setupOmid() throws Exception {
        LOG.info("Setting up OmidTestBase...");

        // TSO Setup
        tso = new TSOTestBase();
        tso.setupTSO();

        // HBase setup
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 100*1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);
        hbaseConf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 3);
        hbaseConf.set("tso.host", "localhost");
        hbaseConf.setInt("tso.port", 1234);
        final String rootdir = "/tmp/hbase.test.dir/";
        File rootdirFile = new File(rootdir);
        if (rootdirFile.exists()) {
            delete(rootdirFile);
        }
        hbaseConf.set("hbase.rootdir", rootdir);

        LOG.info("Create hbase");
        testutil = new HBaseTestingUtility(hbaseConf);
        hbasecluster = testutil.startMiniCluster(5);

        LOG.info("Setup done");
    }

    private static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    protected TransactionManager newTransactionManager() throws Exception {
        return HBaseTransactionManager.newBuilder()
            .withConfiguration(hbaseConf)
            .withCommitTableClient(tso.getCommitTable().getClient().get())
            .withTSOClient(tso.getClient()).build();
    }

    @AfterClass 
    public static void teardownOmid() throws Exception {
        LOG.info("Tearing down OmidTestBase...");
        if (hbasecluster != null) {
            testutil.shutdownMiniCluster();
        }

        tso.teardownTSO();
    }

    @Before
    public void setUp() throws Exception {
        HBaseAdmin admin = testutil.getHBaseAdmin();

        if (!admin.tableExists(TEST_TABLE)) {
            HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
            HColumnDescriptor datafam = new HColumnDescriptor(TEST_FAMILY);
            HColumnDescriptor datafam2 = new HColumnDescriptor(TEST_FAMILY2);
            datafam.setMaxVersions(Integer.MAX_VALUE);
            datafam2.setMaxVersions(Integer.MAX_VALUE);
            desc.addFamily(datafam);
            desc.addFamily(datafam2);

            admin.createTable(desc);
        }

        if (admin.isTableDisabled(TEST_TABLE)) {
            admin.enableTable(TEST_TABLE);
        }
        HTableDescriptor[] tables = admin.listTables();
        for (HTableDescriptor t : tables) {
            LOG.info(t.getNameAsString());
        }

        CreateTable.createTable(hbaseConf, HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME, 1);
    }

    @After
    public void tearDown() {
        try {
            LOG.info("tearing Down");
            HBaseAdmin admin = testutil.getHBaseAdmin();
            admin.disableTable(TEST_TABLE);
            admin.deleteTable(TEST_TABLE);

            admin.disableTable(HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME);
            admin.deleteTable(HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME);

        } catch (Exception e) {
            LOG.error("Error tearing down", e);
        }
    }

    protected static boolean verifyValue(byte[] tableName, byte[] row,
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
