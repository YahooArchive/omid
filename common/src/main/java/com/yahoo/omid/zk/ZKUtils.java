/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ZKUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ZKUtils.class);

    public static CuratorFramework initZKClient(String zkCluster, String namespace, int zkConnectionTimeoutInSec)
            throws IOException, InterruptedException {

        LOG.info("Creating Zookeeper Client connecting to {}", zkCluster);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .namespace(namespace)
                .connectString(zkCluster)
                .retryPolicy(retryPolicy)
                .build();

        zkClient.start();
        if (zkClient.blockUntilConnected(zkConnectionTimeoutInSec, TimeUnit.SECONDS)) {
            LOG.info("Connected to ZK cluster '{}', client in state: [{}]", zkCluster, zkClient.getState());
        } else {
            throw new IOException(String.format("Can't contact ZK cluster '%s' after 10 seconds", zkCluster));
        }

        return zkClient;

    }

}
