package com.yahoo.omid.tso;

import static com.yahoo.omid.ZKConstants.OMID_NAMESPACE;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class ZKModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(ZKModule.class);

    private final TSOServerCommandLineConfig config;

    public ZKModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    public void configure() {
    }

    @Provides
    CuratorFramework provideZookeeperClient() {

        LOG.info("Creating Zookeeper Client connecting to {}", config.getZKCluster());

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return CuratorFrameworkFactory.builder()
                                      .namespace(OMID_NAMESPACE)
                                      .connectString(config.getZKCluster())
                                      .retryPolicy(retryPolicy)
                                      .build();
    }

}
