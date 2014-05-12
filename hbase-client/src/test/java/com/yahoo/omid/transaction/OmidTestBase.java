/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.transaction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.yahoo.omid.transaction.TransactionManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
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
    private static HBaseTestingUtility testutil;
    private static MiniHBaseCluster hbasecluster;
    protected static Configuration hbaseConf;

    protected static final String TEST_TABLE = "test";
    protected static final String TEST_FAMILY = "data";
    protected static final String TEST_FAMILY2 = "data2";

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

    protected TransactionManager newTransactionManager() throws IOException {
        return TransactionManager.newBuilder()
            .withConfiguration(hbaseConf).withTSOClient(tso.getClient()).build();
    }

    @AfterClass 
    public static void teardownOmid() throws Exception {
        LOG.info("Tearing down OmidTestBase...");
        if (hbasecluster != null) {
            testutil.shutdownMiniCluster();
        }

        tso.teardownTSO();
        TestUtils.waitForSocketNotListening("localhost", 1234, 1000);
    }

    @Before
    public void setUp() throws Exception {
        HBaseAdmin admin = testutil.getHBaseAdmin();

        if (!admin.tableExists(TEST_TABLE)) {
            HTableDescriptor desc = new HTableDescriptor(TEST_TABLE);
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

    protected static void dumpTable(String table) throws Exception {
        HTable t = new HTable(hbaseConf, table);
        if (LOG.isTraceEnabled()) {
            ResultScanner rs = t.getScanner(new Scan());
            Result r = rs.next();
            while (r != null) {
                for (KeyValue kv : r.list()) {
                    LOG.trace("KV: " + kv.getKeyString().toString() + " value:" + Bytes.toString(kv.getValue()));
                }
                r = rs.next();
            }
        }
    }

    protected static boolean verifyValue(byte[] table, byte[] row, 
                                         byte[] fam, byte[] col, byte[] value) {
        try {
            HTable t = new HTable(hbaseConf, table);
            Get g = new Get(row).setMaxVersions(1);
            Result r = t.get(g);
            KeyValue kv = r.getColumnLatest(fam, col);

            if (LOG.isTraceEnabled()) {
                LOG.trace("Value for " + Bytes.toString(table) + ":" 
                          + Bytes.toString(row) + ":" + Bytes.toString(fam) 
                          + Bytes.toString(col) + "=>" + Bytes.toString(kv.getValue()) 
                          + " (" + Bytes.toString(value) + " expected)");
            }

            return Bytes.equals(kv.getValue(), value);
        } catch (Exception e) {
            LOG.error("Error reading row " + Bytes.toString(table) + ":" 
                      + Bytes.toString(row) + ":" + Bytes.toString(fam) 
                      + Bytes.toString(col), e);
            return false;
        }
    }
}
