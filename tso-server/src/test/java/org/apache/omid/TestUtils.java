/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid;

import org.apache.commons.io.IOUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * This class contains functionality that is useful for the Omid tests.
 */
public class TestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    private static final int DEFAULT_ZK_PORT = 2181;

    public static int getFreeLocalPort() throws IOException {

        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }

    }

    public static TestingServer provideTestingZKServer(int port) throws Exception {

        return new TestingServer(port);

    }

    public static TestingServer provideTestingZKServer() throws Exception {

        return provideTestingZKServer(DEFAULT_ZK_PORT);

    }

    public static CuratorFramework provideConnectedZKClient(String zkCluster) throws Exception {

        LOG.info("Creating Zookeeper Client connecting to {}", zkCluster);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder().namespace("omid")
                .connectString(zkCluster).retryPolicy(retryPolicy).build();

        LOG.info("Connecting to ZK cluster {}", zkClient.getState());
        zkClient.start();
        zkClient.blockUntilConnected();
        LOG.info("Connection to ZK cluster {}", zkClient.getState());

        return zkClient;
    }

    public static void waitForSocketListening(String host, int port, int sleepTimeMillis)
            throws IOException, InterruptedException {
        while (true) {
            Socket sock = null;
            try {
                sock = new Socket(host, port);
            } catch (IOException e) {
                // ignore as this is expected
                Thread.sleep(sleepTimeMillis);
                continue;
            } finally {
                IOUtils.closeQuietly(sock);
            }
            LOG.info("Host " + host + ":" + port + " is up...");
            break;
        }
    }

    public static void waitForSocketNotListening(String host, int port, int sleepTimeMillis)
            throws IOException, InterruptedException {
        while (true) {
            Socket sock = null;
            try {
                sock = new Socket(host, port);
            } catch (IOException e) {
                // ignore as this is expected
                break;
            } finally {
                IOUtils.closeQuietly(sock);
            }
            Thread.sleep(sleepTimeMillis);
            LOG.info("Host " + host + ":" + port + " is still up...");
        }
    }

}
