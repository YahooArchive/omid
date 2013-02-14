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
package com.yahoo.omid.examples.notifications;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.examples.Constants;
import com.yahoo.omid.notifications.TransactionCommittedRegionCoprocessor;
import com.yahoo.omid.tso.TSOServer;

public class OmidInfrastructure {

    private static final Logger logger = Logger.getLogger(OmidInfrastructure.class);

    private static final ExecutorService bkExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("bk-executor").build());
    private static final ExecutorService tsoExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("tso-executor").build());
    private static LocalHBaseCluster hbasecluster;
    private static Configuration hbaseConf;
    private static HBaseAdmin admin;

    /**
     * Creates the infrastructure required for Omid
     * 
     * @param args
     */
    public static void main(String[] args) throws Exception {

        final CountDownLatch cdl = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                cdl.countDown();
                logger.info("ooo Omid ooo - Omid's Infrastructure for Example App Stopped (with CTRL+C) - ooo Omid ooo");
            }
        });

        logger.info("ooo Omid ooo - Starting Omid's Infrastructure for Example App - ooo Omid ooo");

        // Bookkeeper setup

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
            throw new Exception("Error starting zookeeper/bookkeeper");
        }

        Thread.sleep(1000);

        // TSO Setup

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
        waitForSocketListening("localhost", 1234, 1000);

        // HBase setup
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.coprocessor.region.classes", "com.yahoo.omid.client.regionserver.Compacter");
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 100 * 1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);

        final String rootdir = "/tmp/hbase.test.dir/";
        File rootdirFile = new File(rootdir);
        if (rootdirFile.exists()) {
            delete(rootdirFile);
        }
        hbaseConf.set("hbase.rootdir", rootdir);

        logger.info("Starting HBase");
        hbasecluster = new LocalHBaseCluster(hbaseConf, 1, 1);
        hbasecluster.startup();

        while (hbasecluster.getActiveMaster() == null || !hbasecluster.getActiveMaster().isInitialized()) {
            Thread.sleep(500);
        }

        createTables();

        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            destroyTables();
            teardownOmidInfrastructure();
            logger.info("ooo Omid ooo - Omid's Infrastructure for Example App Stopped - ooo Omid ooo");
        }

    }

    private static void createTables() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        admin = new HBaseAdmin(hbaseConf);

        if (!admin.tableExists(Constants.TABLE)) {
            HTableDescriptor table = new HTableDescriptor(Constants.TABLE);
            // TODO The coprocessor should be added dynamically but I think is not possible in HBase
            table.addCoprocessor(TransactionCommittedRegionCoprocessor.class.getName(), null, Coprocessor.PRIORITY_USER, null);
            HColumnDescriptor columnFamily = new HColumnDescriptor(Constants.COLUMN_FAMILY_1);
            columnFamily.setMaxVersions(Integer.MAX_VALUE);
            table.addFamily(columnFamily);

            admin.createTable(table);
        }

        if (admin.isTableDisabled(Constants.TABLE)) {
            admin.enableTable(Constants.TABLE);
        }
        HTableDescriptor[] tables = admin.listTables();
        for (HTableDescriptor t : tables) {
            logger.info("Tables:" + t.getNameAsString());
        }
    }

    private static void destroyTables() {
        try {
            logger.info("tearing Down");
            admin.disableTable(Constants.TABLE);
            admin.deleteTable(Constants.TABLE);
        } catch (Exception e) {
            logger.error("Error tearing down", e);
        }
    }

    private static void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    public static void teardownOmidInfrastructure() throws Exception {
        logger.info("Tearing down Omid Infrastructure...");
        if (hbasecluster != null) {
            hbasecluster.shutdown();
        }

        tsoExecutor.shutdownNow();
        bkExecutor.shutdownNow();
        waitForSocketNotListening("localhost", 1234, 1000);
    }

    public static void waitForSocketListening(String host, int port, int sleepTimeMillis) throws UnknownHostException,
            IOException, InterruptedException {
        while (true) {
            Socket sock = null;
            try {
                sock = new Socket(host, port);
            } catch (IOException e) {
                // ignore as this is expected
                Thread.sleep(sleepTimeMillis);
                continue;
            } finally {
                if (sock != null) {
                    sock.close();
                }
            }
            logger.info("Host " + host + ":" + port + " is up");
            break;
        }
    }

    public static void waitForSocketNotListening(String host, int port, int sleepTimeMillis)
            throws UnknownHostException, IOException, InterruptedException {
        while (true) {
            Socket sock = null;
            try {
                sock = new Socket(host, port);
            } catch (IOException e) {
                // ignore as this is expected
                break;
            } finally {
                if (sock != null) {
                    sock.close();
                }
            }
            Thread.sleep(sleepTimeMillis);
            logger.info("Host " + host + ":" + port + " is up");
        }
    }
}
