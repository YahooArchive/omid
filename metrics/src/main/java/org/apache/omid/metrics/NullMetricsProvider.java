/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NullMetricsProvider implements MetricsProvider, MetricsRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(NullMetricsProvider.class);

    public NullMetricsProvider() {
    }

    /* ********************************** MetricsProvider interface implementation ********************************** */

    @Override
    public void startMetrics() {
        LOG.info("Null metrics provider started");
    }

    @Override
    public void stopMetrics() {
        LOG.info("Null metrics provider stopped");
    }

    /* ********************************** MetricsRegistry interface implementation ********************************** */

    @Override
    public <T extends Number> void gauge(String name, Gauge<T> gauge) {
    }

    @Override
    public Counter counter(final String name) {
        return new Counter() {

            @Override
            public void inc() {
                // Do nothing
            }

            @Override
            public void inc(long n) {
                // Do nothing
            }

            @Override
            public void dec() {
                // Do nothing
            }

            @Override
            public void dec(long n) {
                // Do nothing
            }

        };

    }

    @Override
    public Timer timer(final String name) {

        return new Timer() {
            @Override
            public void start() {
                // Do nothing
            }

            @Override
            public void stop() {
                // Do nothing
            }

            @Override
            public void update(long duration) {
                // Do nothing
            }
        };

    }

    @Override
    public Meter meter(final String name) {

        return new Meter() {

            @Override
            public void mark() {
                // Do nothing
            }

            @Override
            public void mark(long n) {
                // Do nothing
            }

        };
    }

    @Override
    public Histogram histogram(final String name) {

        return new Histogram() {

            @Override
            public void update(long value) {
                // Do nothing
            }

            @Override
            public void update(int value) {
                // Do nothing
            }
        };
    }

    /* ********************************************** Private methods *********************************************** */

}
