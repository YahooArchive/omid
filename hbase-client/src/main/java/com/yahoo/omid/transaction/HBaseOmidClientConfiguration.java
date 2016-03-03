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
package com.yahoo.omid.transaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.yahoo.omid.YmlUtils;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.tools.hbase.SecureHBaseConfig;
import com.yahoo.omid.tsoclient.OmidClientConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Arrays;
import java.util.Map;

/**
 * Configuration for HBase's Omid client side
 */
public class HBaseOmidClientConfiguration extends SecureHBaseConfig {
    private static final String DEFAULT_CONFIG_FILE_NAME = "hbase-omid-client-config-default.yml";
    private static final String CONFIG_FILE_NAME = "hbase-omid-client-config.yml";
    private Configuration hbaseConfiguration = HBaseConfiguration.create();
    private String commitTableName;
    @Inject
    private OmidClientConfiguration omidClientConfiguration;
    private MetricsRegistry metrics;


    // ----------------------------------------------------------------------------------------------------------------
    // Instantiation
    // ----------------------------------------------------------------------------------------------------------------
    public HBaseOmidClientConfiguration() {
        this(CONFIG_FILE_NAME);
    }

    @VisibleForTesting
    HBaseOmidClientConfiguration(String configFileName) {
        new YmlUtils<Map>().loadDefaultSettings(Arrays.asList(configFileName, DEFAULT_CONFIG_FILE_NAME), this);
    }


    // ----------------------------------------------------------------------------------------------------------------
    // Getters and setters for config params
    // ----------------------------------------------------------------------------------------------------------------

    public Configuration getHBaseConfiguration() {
        return hbaseConfiguration;
    }

    public void setHBaseConfiguration(Configuration hbaseConfiguration) {
        this.hbaseConfiguration = hbaseConfiguration;
    }

    public String getCommitTableName() {
        return commitTableName;
    }

    @Inject(optional = true)
    @Named("omid.client.hbase.commitTableName")
    public void setCommitTableName(String commitTableName) {
        this.commitTableName = commitTableName;
    }

    public OmidClientConfiguration getOmidClientConfiguration() {
        return omidClientConfiguration;
    }

    public void setOmidClientConfiguration(OmidClientConfiguration omidClientConfiguration) {
        this.omidClientConfiguration = omidClientConfiguration;
    }

    public MetricsRegistry getMetrics() {
        return metrics;
    }

    @Inject(optional = true)
    @Named("omid.client.hbase.metrics")
    public void setMetrics(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

    //delegation to make end-user life better

    public OmidClientConfiguration.ConnType getConnectionType() {
        return omidClientConfiguration.getConnectionType();
    }

    public void setReconnectionDelaySecs(int reconnectionDelaySecs) {
        omidClientConfiguration.setReconnectionDelaySecs(reconnectionDelaySecs);
    }

    public void setExecutorThreads(int executorThreads) {
        omidClientConfiguration.setExecutorThreads(executorThreads);
    }

    public int getRequestTimeoutMs() {
        return omidClientConfiguration.getRequestTimeoutMs();
    }

    public void setConnectionString(String connectionString) {
        omidClientConfiguration.setConnectionString(connectionString);
    }

    public void setRequestTimeoutMs(int requestTimeoutMs) {
        omidClientConfiguration.setRequestTimeoutMs(requestTimeoutMs);
    }

    public void setZkConnectionTimeoutSecs(int zkConnectionTimeoutSecs) {
        omidClientConfiguration.setZkConnectionTimeoutSecs(zkConnectionTimeoutSecs);
    }

    public void setConnectionType(OmidClientConfiguration.ConnType connectionType) {
        omidClientConfiguration.setConnectionType(connectionType);
    }

    public void setRequestMaxRetries(int requestMaxRetries) {
        omidClientConfiguration.setRequestMaxRetries(requestMaxRetries);
    }

    public int getZkConnectionTimeoutSecs() {
        return omidClientConfiguration.getZkConnectionTimeoutSecs();
    }

    public void setRetryDelayMs(int retryDelayMs) {
        omidClientConfiguration.setRetryDelayMs(retryDelayMs);
    }

    public int getExecutorThreads() {
        return omidClientConfiguration.getExecutorThreads();
    }

    public int getRetryDelayMs() {
        return omidClientConfiguration.getRetryDelayMs();
    }

    public String getConnectionString() {
        return omidClientConfiguration.getConnectionString();
    }

    public int getRequestMaxRetries() {
        return omidClientConfiguration.getRequestMaxRetries();
    }

    public int getReconnectionDelaySecs() {
        return omidClientConfiguration.getReconnectionDelaySecs();
    }
}
