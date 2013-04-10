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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.yahoo.omid.notifications.conf.DeltaOmidServerConfig;
import com.yahoo.omid.notifications.conf.ServerConfiguration;
import com.yahoo.omid.notifications.metrics.MetricsUtils;

public class DeltaOmidServer {

    private static final Logger logger = LoggerFactory.getLogger(DeltaOmidServer.class);

    private static CuratorFramework zkClient;

    private static ScannerSandbox scannerSandbox;

    private static AppSandbox appSandbox;

    /**
     * This is where all starts...
     * 
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("Uncaught exception in thread {}", t.getName(), e);

            }
        });

        DeltaOmidServerConfig conf = DeltaOmidServerConfig.parseConfig(args);

        ServerConfiguration serverConfiguration = new ServerConfiguration();
        if (serverConfiguration.containsKey("omid.metrics")) {
            MetricsUtils.initMetrics(serverConfiguration.getString("omid.metrics"));
        }
        logger.info("ooo Omid ooo - Starting Delta Omid Notification Server - ooo Omid ooo");

        // TODO enable configurable reconnection parameters
        zkClient = CuratorFrameworkFactory.newClient(conf.getZkServers(), new ExponentialBackoffRetry(10000, 3));
        zkClient.start();
        logger.info("ZK client started");

        scannerSandbox = new ScannerSandbox(conf);
        logger.info("Scanner Sandbox started");

        appSandbox = new AppSandbox(zkClient, scannerSandbox);
        logger.info("App Sandbox started");

        createOrRecoverServerConfigFromZkTree();

        logger.info("ooo Omid ooo - Delta Omid Notification Server started - ooo Omid ooo");

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await();

    }

    private static void createOrRecoverServerConfigFromZkTree() throws Exception {
        logger.info("Configuring server based on current ZK structure");
        Stat s = zkClient.checkExists().forPath(ZkTreeUtils.getRootNodePath());
        if (s == null) {
            createZkTree();
        } else {
            recoverCurrentZkAppBranch();
        }
        appSandbox.startWatchingAppsNode();
        logger.info("Server configuration finished");
    }

    private static void createZkTree() throws Exception {
        logger.info("Creating a new ZK Tree");
        zkClient.create().creatingParentsIfNeeded().forPath(ZkTreeUtils.getAppsNodePath());
        zkClient.create().creatingParentsIfNeeded().forPath(ZkTreeUtils.getServersNodePath());
    }

    private static void recoverCurrentZkAppBranch() throws Exception {
        logger.info("Recovering existing ZK Tree");
        String appsNodePath = ZkTreeUtils.getAppsNodePath();
        List<String> appNames = zkClient.getChildren().forPath(appsNodePath);
        for (String appName : appNames) {
            appSandbox.createApplication(appName);
        }
    }

    public static boolean waitForServerUp(String targetHostPort, long timeout) {
        long start = System.currentTimeMillis();
        String split[] = targetHostPort.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);
        while (true) {
            try {
                Socket sock = new Socket(host, port);
                BufferedReader reader = null;
                try {
                    OutputStream outstream = sock.getOutputStream();
                    outstream.write("stat".getBytes());
                    outstream.flush();

                    reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                    String line = reader.readLine();
                    if (line != null && line.startsWith("Zookeeper version:")) {
                        logger.info("Server UP");
                        return true;
                    }
                } finally {
                    sock.close();
                    if (reader != null) {
                        reader.close();
                    }
                }
            } catch (IOException e) {
                // ignore as this is expected
                logger.info("Server " + targetHostPort + " not up " + e);
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }

}
