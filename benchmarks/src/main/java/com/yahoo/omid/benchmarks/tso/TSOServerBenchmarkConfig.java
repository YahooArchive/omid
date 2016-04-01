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
package com.yahoo.omid.benchmarks.tso;

import com.google.inject.AbstractModule;
import com.yahoo.omid.YAMLUtils;
import com.yahoo.omid.benchmarks.utils.IntegerGenerator;
import com.yahoo.omid.benchmarks.utils.TimeValueTimeUnit;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.tools.hbase.SecureHBaseConfig;
import com.yahoo.omid.tsoclient.OmidClientConfiguration;

public class TSOServerBenchmarkConfig extends SecureHBaseConfig {

    private static final String DEFAULT_CONFIG_FILE_NAME = "tso-server-benchmark-config.yml";
    private static final String CONFIG_FILE_NAME = "default-tso-server-benchmark-config.yml";

    private TimeValueTimeUnit runFor;

    private int txRunners;
    private int txRateInRequestPerSecond;
    private TimeValueTimeUnit warmUp;
    private IntegerGenerator cellIdGenerator;
    private int writesetSize;
    private boolean fixedWritesetSize;
    private int percentageOfReadOnlyTxs;
    private TimeValueTimeUnit commitDelay;

    private OmidClientConfiguration omidClientConfiguration;
    private AbstractModule commitTableStoreModule;

    private MetricsRegistry metrics;

    // ----------------------------------------------------------------------------------------------------------------
    // Instantiation
    // ----------------------------------------------------------------------------------------------------------------

    TSOServerBenchmarkConfig() {
        this(CONFIG_FILE_NAME);
    }

    TSOServerBenchmarkConfig(String configFileName) {
        new YAMLUtils().loadSettings(DEFAULT_CONFIG_FILE_NAME, configFileName, this);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Getters and setters for config params
    // ----------------------------------------------------------------------------------------------------------------

    public TimeValueTimeUnit getRunFor() {
        return runFor;
    }

    public void setRunFor(TimeValueTimeUnit runFor) {
        this.runFor = runFor;
    }

    public int getTxRunners() {
        return txRunners;
    }

    public void setTxRunners(int txRunners) {
        this.txRunners = txRunners;
    }

    public int getTxRateInRequestPerSecond() {
        return txRateInRequestPerSecond;
    }

    public void setTxRateInRequestPerSecond(int txRateInRequestPerSecond) {
        this.txRateInRequestPerSecond = txRateInRequestPerSecond;
    }

    public TimeValueTimeUnit getWarmUp() {
        return warmUp;
    }

    public void setWarmUp(TimeValueTimeUnit warmUp) {
        this.warmUp = warmUp;
    }

    public IntegerGenerator getCellIdGenerator() {
        return cellIdGenerator;
    }

    public void setCellIdGenerator(IntegerGenerator cellIdGenerator) {
        this.cellIdGenerator = cellIdGenerator;
    }

    public int getWritesetSize() {
        return writesetSize;
    }

    public void setWritesetSize(int writesetSize) {
        this.writesetSize = writesetSize;
    }

    public boolean isFixedWritesetSize() {
        return fixedWritesetSize;
    }

    public void setFixedWritesetSize(boolean fixedWritesetSize) {
        this.fixedWritesetSize = fixedWritesetSize;
    }

    public int getPercentageOfReadOnlyTxs() {
        return percentageOfReadOnlyTxs;
    }

    public void setPercentageOfReadOnlyTxs(int percentageOfReadOnlyTxs) {
        this.percentageOfReadOnlyTxs = percentageOfReadOnlyTxs;
    }

    public TimeValueTimeUnit getCommitDelay() {
        return commitDelay;
    }

    public void setCommitDelay(TimeValueTimeUnit commitDelay) {
        this.commitDelay = commitDelay;
    }

    public OmidClientConfiguration getOmidClientConfiguration() {
        return omidClientConfiguration;
    }

    public void setOmidClientConfiguration(OmidClientConfiguration omidClientConfiguration) {
        this.omidClientConfiguration = omidClientConfiguration;
    }

    public AbstractModule getCommitTableStoreModule() {
        return commitTableStoreModule;
    }

    public void setCommitTableStoreModule(AbstractModule commitTableStoreModule) {
        this.commitTableStoreModule = commitTableStoreModule;
    }

    public MetricsRegistry getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

}
