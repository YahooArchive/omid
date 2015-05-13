/**
 * Copyright 2011-2015 Yahoo Inc.
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
