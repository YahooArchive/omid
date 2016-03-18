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
package com.yahoo.omid.tsoclient;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.yahoo.omid.YAMLUtils;

/**
 * Configuration for Omid client side
 */
public class OmidClientConfiguration {
    private static final String DEFAULT_CONFIG_FILE_NAME = "omid-client-config.yml";

    public enum ConnType {DIRECT, HA}

    private ConnType connectionType = ConnType.DIRECT;
    private String connectionString;
    private String zkCurrentTsoPath;
    private String zkNamespace;
    private int zkConnectionTimeoutSecs;
    private int requestMaxRetries;
    private int requestTimeoutMs;
    private int reconnectionDelaySecs;
    private int retryDelayMs;
    private int executorThreads;

    public OmidClientConfiguration() {
        new YAMLUtils().loadSettings(DEFAULT_CONFIG_FILE_NAME, this);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters and setters for config params
    // ----------------------------------------------------------------------------------------------------------------

    public ConnType getConnectionType() {
        return connectionType;
    }

    @Inject(optional = true)
    @Named("omid.client.connectionType")
    public void setConnectionType(ConnType connectionType) {
        this.connectionType = connectionType;
    }

    public String getConnectionString() {
        return connectionString;
    }

    @Inject(optional = true)
    @Named("omid.client.connectionString")
    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public int getZkConnectionTimeoutSecs() {
        return zkConnectionTimeoutSecs;
    }

    @Inject(optional = true)
    @Named("omid.client.zkConnectionTimeoutSecs")
    public void setZkConnectionTimeoutSecs(int zkConnectionTimeoutSecs) {
        this.zkConnectionTimeoutSecs = zkConnectionTimeoutSecs;
    }

    public int getRequestMaxRetries() {
        return requestMaxRetries;
    }

    @Inject(optional = true)
    @Named("omid.client.requestMaxRetries")
    public void setRequestMaxRetries(int requestMaxRetries) {
        this.requestMaxRetries = requestMaxRetries;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    @Inject(optional = true)
    @Named("omid.client.requestTimeoutMs")
    public void setRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public int getReconnectionDelaySecs() {
        return reconnectionDelaySecs;
    }

    @Inject(optional = true)
    @Named("omid.client.reconnectionDelaySecs")
    public void setReconnectionDelaySecs(int reconnectionDelaySecs) {
        this.reconnectionDelaySecs = reconnectionDelaySecs;
    }

    public int getRetryDelayMs() {
        return retryDelayMs;
    }

    @Inject(optional = true)
    @Named("omid.client.retryDelayMs")
    public void setRetryDelayMs(int retryDelayMs) {
        this.retryDelayMs = retryDelayMs;
    }

    public int getExecutorThreads() {
        return executorThreads;
    }

    @Inject(optional = true)
    @Named("omid.client.executorThreads")
    public void setExecutorThreads(int executorThreads) {
        this.executorThreads = executorThreads;
    }

    public String getZkCurrentTsoPath() {
        return zkCurrentTsoPath;
    }

    @Inject(optional = true)
    @Named("omid.ha.zkCurrentTsoPath")
    public void setZkCurrentTsoPath(String zkCurrentTsoPath) {
        this.zkCurrentTsoPath = zkCurrentTsoPath;
    }

    public String getZkNamespace() {
        return zkNamespace;
    }

    @Inject(optional = true)
    @Named("omid.ha.zkNamespace")
    public void setZkNamespace(String zkNamespace) {
        this.zkNamespace = zkNamespace;
    }
}
