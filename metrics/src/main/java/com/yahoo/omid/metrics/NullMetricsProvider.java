package com.yahoo.omid.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NullMetricsProvider implements MetricsProvider, MetricsRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(NullMetricsProvider.class);

    public NullMetricsProvider() {}

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
