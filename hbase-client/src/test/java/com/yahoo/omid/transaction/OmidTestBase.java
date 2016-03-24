package com.yahoo.omid.transaction;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.InMemoryCommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTableConfig;
import com.yahoo.omid.timestamp.storage.HBaseTimestampStorageConfig;
import com.yahoo.omid.tools.hbase.OmidTableManager;
import com.yahoo.omid.tso.TSOMockModule;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerConfig;
import com.yahoo.omid.tsoclient.OmidClientConfiguration;
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
import org.testng.ITestContext;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;

public abstract class OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OmidTestBase.class);

    static HBaseTestingUtility hBaseUtils;
    private static MiniHBaseCluster hbaseCluster;
    static Configuration hbaseConf;

    protected static final String TEST_TABLE = "test";
    protected static final String TEST_FAMILY = "data";
    static final String TEST_FAMILY2 = "data2";
    private HBaseCommitTableConfig hBaseCommitTableConfig;

    @BeforeMethod(alwaysRun = true)
    public void beforeClass(Method method) throws Exception {
        Thread.currentThread().setName("UnitTest-" + method.getName());
    }


    @BeforeGroups(groups = "sharedHBase")
    public void beforeGroups(ITestContext context) throws Exception {
        // TSO Setup
        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setPort(1234);
        tsoConfig.setMaxItems(1000);
        Injector injector = Guice.createInjector(new TSOMockModule(tsoConfig));
        LOG.info("Starting TSO");
        TSOServer tso = injector.getInstance(TSOServer.class);
        hBaseCommitTableConfig = injector.getInstance(HBaseCommitTableConfig.class);
        HBaseTimestampStorageConfig hBaseTimestampStorageConfig = injector.getInstance(HBaseTimestampStorageConfig.class);
        tso.startAndWait();
        TestUtils.waitForSocketListening("localhost", 1234, 100);
        LOG.info("Finished loading TSO");
        context.setAttribute("tso", tso);

        OmidClientConfiguration clientConf = new OmidClientConfiguration();
        clientConf.setConnectionString("localhost:1234");
        context.setAttribute("clientConf", clientConf);

        InMemoryCommitTable commitTable = (InMemoryCommitTable) injector.getInstance(CommitTable.class);
        context.setAttribute("commitTable", commitTable);

        // Create the associated Handler
        TSOClient client = TSOClient.newInstance(clientConf);
        context.setAttribute("client", client);

        // ------------------------------------------------------------------------------------------------------------
        // HBase setup
        // ------------------------------------------------------------------------------------------------------------
        LOG.info("Creating HBase minicluster");
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 10_000 * 1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);
        hbaseConf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 3);

        File tempFile = File.createTempFile("OmidTest", "");
        tempFile.deleteOnExit();
        hbaseConf.set("hbase.rootdir", tempFile.getAbsolutePath());

        hBaseUtils = new HBaseTestingUtility(hbaseConf);
        hbaseCluster = hBaseUtils.startMiniCluster(1);
        hBaseUtils.createTable(Bytes.toBytes(hBaseTimestampStorageConfig.getTableName()),
                               new byte[][]{hBaseTimestampStorageConfig.getFamilyName().getBytes()},
                               Integer.MAX_VALUE);

        createTestTable();
        createCommitTable();

        LOG.info("HBase minicluster is up");
    }

    private void createTestTable() throws IOException {
        HBaseAdmin admin = hBaseUtils.getHBaseAdmin();
        HTableDescriptor test_table_desc = new HTableDescriptor(TableName.valueOf(TEST_TABLE));
        HColumnDescriptor datafam = new HColumnDescriptor(TEST_FAMILY);
        HColumnDescriptor datafam2 = new HColumnDescriptor(TEST_FAMILY2);
        datafam.setMaxVersions(Integer.MAX_VALUE);
        datafam2.setMaxVersions(Integer.MAX_VALUE);
        test_table_desc.addFamily(datafam);
        test_table_desc.addFamily(datafam2);
        admin.createTable(test_table_desc);
    }

    private void createCommitTable() throws IOException {
        String[] args = new String[]{OmidTableManager.COMMIT_TABLE_COMMAND_NAME, "-numRegions", "1"};
        OmidTableManager omidTableManager = new OmidTableManager(args);
        omidTableManager.executeActionsOnHBase(hbaseConf);
    }


    private TSOServer getTSO(ITestContext context) {
        return (TSOServer) context.getAttribute("tso");
    }


    TSOClient getClient(ITestContext context) {
        return (TSOClient) context.getAttribute("client");
    }

    InMemoryCommitTable getCommitTable(ITestContext context) {
        return (InMemoryCommitTable) context.getAttribute("commitTable");
    }

    protected TransactionManager newTransactionManager(ITestContext context) throws Exception {
        return newTransactionManager(context, getClient(context));
    }

    protected TransactionManager newTransactionManager(ITestContext context, PostCommitActions postCommitActions) throws Exception {
        HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
        clientConf.setConnectionString("localhost:1234");
        clientConf.setHBaseConfiguration(hbaseConf);
        return HBaseTransactionManager.builder(clientConf)
                .postCommitter(postCommitActions)
                .commitTableClient(getCommitTable(context).getClient())
                .tsoClient(getClient(context)).build();
    }

    protected TransactionManager newTransactionManager(ITestContext context, TSOClient tsoClient) throws Exception {
        HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
        clientConf.setConnectionString("localhost:1234");
        clientConf.setHBaseConfiguration(hbaseConf);
        return HBaseTransactionManager.builder(clientConf)
                .commitTableClient(getCommitTable(context).getClient())
                .tsoClient(tsoClient).build();
    }

    protected TransactionManager newTransactionManager(ITestContext context, CommitTable.Client commitTableClient)
            throws Exception {
        HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
        clientConf.setConnectionString("localhost:1234");
        clientConf.setHBaseConfiguration(hbaseConf);
        return HBaseTransactionManager.builder(clientConf)
                .commitTableClient(commitTableClient)
                .tsoClient(getClient(context)).build();
    }

    @AfterGroups(groups = "sharedHBase")
    public void afterGroups(ITestContext context) throws Exception {
        LOG.info("Tearing down OmidTestBase...");
        if (hbaseCluster != null) {
            hBaseUtils.shutdownMiniCluster();
        }

        getClient(context).close().get();
        getTSO(context).stopAndWait();
        TestUtils.waitForSocketNotListening("localhost", 1234, 1000);
    }

    @AfterMethod(groups = "sharedHBase", timeOut = 60_000)
    public void afterMethod() {
        try {
            LOG.info("tearing Down");
            HBaseAdmin admin = hBaseUtils.getHBaseAdmin();
            deleteTable(admin, TableName.valueOf(TEST_TABLE));
            createTestTable();
            deleteTable(admin, TableName.valueOf(hBaseCommitTableConfig.getTableName()));
            createCommitTable();
        } catch (Exception e) {
            LOG.error("Error tearing down", e);
        }
    }

    void deleteTable(HBaseAdmin admin, TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            if (admin.isTableDisabled(tableName)) {
                admin.deleteTable(tableName);
            } else {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        }
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
