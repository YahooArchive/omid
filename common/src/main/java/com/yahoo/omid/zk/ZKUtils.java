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

import static com.yahoo.omid.ZKConstants.OMID_NAMESPACE;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ZKUtils.class);

    public static CuratorFramework provideZookeeperClient(String zkCluster) {

        LOG.info("Creating Zookeeper Client connecting to {}", zkCluster);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return CuratorFrameworkFactory.builder()
                                      .namespace(OMID_NAMESPACE)
                                      .connectString(zkCluster)
                                      .retryPolicy(retryPolicy)
                                      .build();
    }

    /**
     * Thrown when a problem with ZK is found
     */
    public static class ZKException extends Exception {

        private static final long serialVersionUID = -4680106966051809489L;

        public ZKException(String message) {
            super(message);
        }

    }


}
