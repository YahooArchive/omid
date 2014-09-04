package com.yahoo.omid.timestamp.storage;

import javax.inject.Singleton;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;

public class ZKTimestampStorageModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(ZKTimestampStorageModule.class);

    private final TSOServerCommandLineConfig config;

    public ZKTimestampStorageModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    public void configure() {
        // Timestamp storage creation
        bind(TimestampStorage.class).to(ZKTimestampStorage.class).in(Singleton.class);

    }

    @Provides
    CuratorFramework provideZookeeperClient() {

        LOG.info("Creating Zookeeper Client connecting to {}", config.getZKCluster());

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return CuratorFrameworkFactory.builder()
                                      .namespace("omid")
                                      .connectString(config.getZKCluster())
                                      .retryPolicy(retryPolicy)
                                      .build();
    }

}
