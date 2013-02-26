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
package com.yahoo.omid.notifications;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.yahoo.omid.TestUtils;
import com.yahoo.omid.tso.TSOServer;

public class TestInfrastructure {

    private static final Logger logger = Logger.getLogger(TestInfrastructure.class);
    
    private static final ExecutorService bkExecutor = Executors.newSingleThreadExecutor();
    private static final ExecutorService tsoExecutor = Executors.newSingleThreadExecutor();
    private static final ExecutorService nsExecutor = Executors.newSingleThreadExecutor();
    private static LocalHBaseCluster hbasecluster;
    protected static Configuration hbaseConf;
    private static HBaseAdmin admin;
    
    @BeforeClass 
    public static void startServices() throws Exception {
        logger.info("Starting up Notification Infrastructure...");
        setupBK();
        setupTSO();
        setupHBase();
        setupNotificationServer();
        logger.info("Notification Infrastructure Started...");
    }
    
    @AfterClass 
    public static void stopServices() throws Exception {
        logger.info("Tearing down Notification Infrastructure...");
        if (hbasecluster != null) {
           hbasecluster.shutdown();
        }

        nsExecutor.shutdownNow();
        tsoExecutor.shutdownNow();
        bkExecutor.shutdownNow();
        TestUtils.waitForSocketNotListening("localhost", 1234, 1000);
    }
    
    @Before
    public void setUp() throws Exception {
        admin = new HBaseAdmin(hbaseConf);

        if (!admin.tableExists(TestConstants.TABLE)) {
            HTableDescriptor table = new HTableDescriptor(TestConstants.TABLE);
            // TODO The coprocessor should be added dynamically but I think is not possible in HBase
            table.addCoprocessor(TransactionCommittedRegionCoprocessor.class.getName(), null, Coprocessor.PRIORITY_USER, null);
            HColumnDescriptor columnFamily = new HColumnDescriptor(TestConstants.COLUMN_FAMILY_1);
            columnFamily.setMaxVersions(Integer.MAX_VALUE);
            table.addFamily(columnFamily);

            admin.createTable(table);
        }

        if (admin.isTableDisabled(TestConstants.TABLE)) {
            admin.enableTable(TestConstants.TABLE);
        }
        HTableDescriptor[] tables = admin.listTables();
        for (HTableDescriptor t : tables) {
            logger.info("Tables:" + t.getNameAsString());
        }

    }

    @After
    public void tearDown() {
        try {
            logger.info("tearing Down");
            admin.disableTable(TestConstants.TABLE);
            admin.deleteTable(TestConstants.TABLE);
        } catch (Exception e) {
            logger.error("Error tearing down", e);
        }
    }
    
    private static void setupBK() throws Exception {
        Runnable bkTask = new Runnable() {
            public void run() {
                try {
                    logger.info("Starting BookKeeper");
                    String[] args = new String[] { "5" };
                    LocalBookKeeper.main(args);
                } catch (InterruptedException e) {
                    // go away quietly
                } catch (Exception e) {
                    logger.error("Error starting local BookKeeper", e);
                }
            }
        };

        bkExecutor.execute(bkTask);
        if (!LocalBookKeeper.waitForServerUp("localhost:2181", 10000)) {
            throw new Exception("Error starting BookKeeper");
        }
    }
    
    private static void setupTSO() throws Exception {
        Runnable tsoTask = new Runnable() {
            public void run() {
                try {
                    logger.info("Starting TSO");
                    String[] args = new String[] { "-port", "1234", "-batch", "100", "-ensemble", "4", "-quorum", "2",
                            "-zk", "localhost:2181" };
                    TSOServer.main(args);
                } catch (InterruptedException e) {
                    // go away quietly
                } catch (Exception e) {
                    logger.error("Error starting TSO", e);
                }
            }
        };
        
        tsoExecutor.execute(tsoTask);
        TestUtils.waitForSocketListening("localhost", 1234, 1000);
        
    }

    private static void setupHBase() throws Exception {
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.coprocessor.region.classes", "com.yahoo.omid.client.regionserver.Compacter");
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 100 * 1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);
        hbaseConf.set("tso.host", "localhost");
        hbaseConf.setInt("tso.port", 1234);

        File hbaseDataDir = new File(TestConstants.HBASE_DATA);
        if (hbaseDataDir.exists()) {
            TestUtils.delete(hbaseDataDir);
        }
        hbaseConf.set("hbase.rootdir", TestConstants.HBASE_DATA);

        logger.info("Starting HBase");
        hbasecluster = new LocalHBaseCluster(hbaseConf, 1, 1);
        hbasecluster.startup();

        while (hbasecluster.getActiveMaster() == null || !hbasecluster.getActiveMaster().isInitialized()) {
            Thread.sleep(500);
        }
    }
    
    private static void setupNotificationServer() throws Exception {
        Runnable notificationServerbkTask = new Runnable() {
            public void run() {
                try {
                    logger.info("Starting Notification Server");
                    String[] args = new String[] {};
                    NotificationServer.main(args);
                } catch (InterruptedException e) {
                    // go away quietly
                } catch (Exception e) {
                    logger.error("Error starting local Notification Server", e);
                }
            }
        };

        nsExecutor.execute(notificationServerbkTask);
        if (!NotificationServer.waitForServerUp("localhost:2181", 10000)) {
            throw new Exception("Error starting Notification Server");
        }
    }
    
}
