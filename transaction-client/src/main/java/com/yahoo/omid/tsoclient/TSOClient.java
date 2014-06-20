/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tsoclient;

import com.codahale.metrics.MetricRegistry;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * Communication endpoint for TSO clients.
 *
 */
public abstract class TSOClient {
    public static final String TSO_HOST_CONFKEY = "tso.host";
    public static final String TSO_PORT_CONFKEY = "tso.port";
    public static final String REQUEST_MAX_RETRIES_CONFKEY = "request.max-retries";
    public static final String REQUEST_TIMEOUT_IN_MS_CONFKEY = "request.timeout-ms";
    public static final int DEFAULT_TSO_PORT = 54758;
    public static final int DEFAULT_TSO_MAX_REQUEST_RETRIES = 5;
    public static final int DEFAULT_REQUEST_TIMEOUT_MS = 5000; // 5 secs
    public static final String TSO_RETRY_DELAY_MS_CONFKEY = "tso.retry_delay_ms";
    public static final int DEFAULT_TSO_RETRY_DELAY_MS = 1000;
    public static final String TSO_EXECUTOR_THREAD_NUM_CONFKEY = "tso.executor.threads";
    public static final int DEFAULT_TSO_EXECUTOR_THREAD_NUM = 3;
    public static class ConnectionException extends IOException {
    }

    public static class ClosingException extends Exception {
    }

    public static class ServiceUnavailableException extends Exception {
    }

    public static class AbortException extends Exception {}

    public static class Builder {
        private Configuration conf = new BaseConfiguration();
        private MetricRegistry metrics = new MetricRegistry();
        private String tsoHost = null;
        private int tsoPort = -1;

        public Builder withConfiguration(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public Builder withMetrics(MetricRegistry metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder withTSOHost(String host) {
            this.tsoHost = host;
            return this;
        }

        public Builder withTSOPort(int port) {
            this.tsoPort = port;
            return this;
        }

        public TSOClient build() {
            if (tsoHost != null) {
                conf.setProperty(TSO_HOST_CONFKEY, tsoHost);
            }
            if (tsoPort != -1) {
                conf.setProperty(TSO_PORT_CONFKEY, tsoPort);
            }
            return new TSOClientImpl(conf, metrics);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public abstract TSOFuture<Long> getNewStartTimestamp();
    public abstract TSOFuture<Long> commit(long transactionId, Set<? extends CellId> cells);
    public abstract TSOFuture<Void> close();
}
