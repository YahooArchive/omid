package com.yahoo.omid.transaction;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.InMemoryCommitTable;
import com.yahoo.omid.committable.hbase.CommitTableConstants;
import com.yahoo.omid.tools.hbase.OmidTableManager;
import com.yahoo.omid.tso.TSOMockModule;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import com.yahoo.omid.tsoclient.TSOClient;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
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

import static com.yahoo.omid.timestamp.storage.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.timestamp.storage.HBaseTimestampStorage.TSO_FAMILY;
import static com.yahoo.omid.tools.hbase.OmidTableManager.COMMIT_TABLE_COMMAND_NAME;
import static com.yahoo.omid.tsoclient.TSOClient.ZK_CONNECTION_TIMEOUT_IN_SECS_CONFKEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;

public abstract class OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(OmidTestBase.class);

    static HBaseTestingUtility hBaseUtils;
    static MiniHBaseCluster hbaseCluster;
    static Configuration hbaseConf;

    protected static final String TEST_TABLE = "test";
    protected static final String TEST_FAMILY = "data";
    static final String TEST_FAMILY2 = "data2";

    @BeforeMethod(alwaysRun = true)
    public void beforeClass(Method method) throws Exception {
        Thread.currentThread().setName("UnitTest-" + method.getName());
    }


    @BeforeGroups(groups = "sharedHBase")
    public void beforeGroups(ITestContext context) throws Exception {
        // TSO Setup
        Injector
            injector =
            Guice.createInjector(new TSOMockModule(TSOServerCommandLineConfig.configFactory(1234, 1000)));
        LOG.info("Starting TSO");
        TSOServer tso = injector.getInstance(TSOServer.class);
        tso.startAndWait();
        TestUtils.waitForSocketListening("localhost", 1234, 100);
        LOG.info("Finished loading TSO");
        context.setAttribute("tso", tso);

        org.apache.commons.configuration.Configuration clientConf = new BaseConfiguration();
        clientConf.setProperty("tso.host", "localhost");
        clientConf.setProperty("tso.port", 1234);
        clientConf.setProperty(ZK_CONNECTION_TIMEOUT_IN_SECS_CONFKEY, 0);
        context.setAttribute("clientConf", clientConf);

        InMemoryCommitTable commitTable = (InMemoryCommitTable) injector.getInstance(CommitTable.class);
        context.setAttribute("commitTable", commitTable);

        // Create the associated Handler
        TSOClient client = TSOClient.newBuilder().withConfiguration(clientConf).build();
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

        HBaseAdmin admin = hBaseUtils.getHBaseAdmin();
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TEST_TABLE));
        HColumnDescriptor datafam = new HColumnDescriptor(TEST_FAMILY);
        HColumnDescriptor datafam2 = new HColumnDescriptor(TEST_FAMILY2);
        datafam.setMaxVersions(Integer.MAX_VALUE);
        datafam2.setMaxVersions(Integer.MAX_VALUE);
        desc.addFamily(datafam);
        desc.addFamily(datafam2);

        admin.createTable(desc);

        hBaseUtils
            .createTable(Bytes.toBytes(TIMESTAMP_TABLE_DEFAULT_NAME), new byte[][]{TSO_FAMILY}, Integer.MAX_VALUE);

        // Create commit table
        String[] args = new String[]{COMMIT_TABLE_COMMAND_NAME, "-numRegions", "1"};
        OmidTableManager omidTableManager = new OmidTableManager(args);
        omidTableManager.executeActionsOnHBase(hbaseConf);

        LOG.info("HBase minicluster is up");
    }


    TSOServer getTSO(ITestContext context) {
        return (TSOServer) context.getAttribute("tso");
    }


    TSOClient getClient(ITestContext context) {
        return (TSOClient) context.getAttribute("client");
    }

    org.apache.commons.configuration.Configuration getClientConfiguration(ITestContext context) {
        return (org.apache.commons.configuration.Configuration) context.getAttribute("clientConf");
    }

    InMemoryCommitTable getCommitTable(ITestContext context) {
        return (InMemoryCommitTable) context.getAttribute("commitTable");
    }

    protected TransactionManager newTransactionManager(ITestContext context) throws Exception {
        return newTransactionManager(context, getClient(context));
    }

    protected TransactionManager newTransactionManager(ITestContext context, TSOClient tsoClient) throws Exception {
        return HBaseTransactionManager.newBuilder()
            .withConfiguration(hbaseConf)
            .withCommitTableClient(getCommitTable(context).getClient().get())
            .withTSOClient(tsoClient).build();
    }

    protected TransactionManager newTransactionManager(ITestContext context, CommitTable.Client commitTableClient)
        throws Exception {
        return HBaseTransactionManager.newBuilder()
            .withConfiguration(hbaseConf)
            .withCommitTableClient(commitTableClient)
            .withTSOClient(getClient(context)).build();
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

    @AfterMethod(groups = "sharedHBase", timeOut = 30_000)
    public void afterMethod() {
        try {
            LOG.info("tearing Down");
            HBaseAdmin admin = hBaseUtils.getHBaseAdmin();
            truncateTable(admin, TableName.valueOf(TEST_TABLE));
            truncateTable(admin, TableName.valueOf(CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME));
        } catch (Exception e) {
            LOG.error("Error tearing down", e);
        }
    }

    void truncateTable(HBaseAdmin admin, TableName tableName) throws IOException {
        try {
            admin.truncateTable(tableName, true);
        } catch (TableNotDisabledException e) {
            admin.disableTable(tableName);
            admin.truncateTable(tableName, true);
            if (admin.isTableDisabled(tableName)) {
                admin.enableTable(tableName);
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
