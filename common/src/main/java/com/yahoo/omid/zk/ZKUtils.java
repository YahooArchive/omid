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
