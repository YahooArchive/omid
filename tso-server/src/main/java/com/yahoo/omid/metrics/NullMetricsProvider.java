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
        LOG.trace("Null gauge (name={}) registered", name);
    }

    @Override
    public Counter counter(final String name) {

        LOG.trace("Null counter (name={}) registered", name);

        return new Counter() {

            @Override
            public void inc() {
                LOG.trace("Null counter (name={}) incremented", name);
                // Do nothing
            }

            @Override
            public void inc(long n) {
                LOG.trace("Null counter (name={}) incremented by {}", name, n);
                // Do nothing
            }

            @Override
            public void dec() {
                LOG.trace("Null counter (name={}) decremented", name);
                // Do nothing
            }

            @Override
            public void dec(long n) {
                LOG.trace("Null counter (name={}) decremented by {}", name, n);
                // Do nothing
            }

        };

    }

    @Override
    public Timer timer(final String name) {

        LOG.trace("Null timer (name={}) registered", name);

        return new Timer() {
            @Override
            public void start() {
                LOG.trace("Null timer (name={}) started", name);
                // Do nothing
            }

            @Override
            public void stop() {
                LOG.trace("Null timer (name={}) stopped", name);
                // Do nothing
            }

            @Override
            public void update(long duration) {
                LOG.trace("Null timer (name={}) updated with duration {}", name, duration);
                // Do nothing
            }
        };

    }

    @Override
    public Meter meter(final String name) {

        LOG.trace("Null meter (name={}) registered", name);

        return new Meter() {

            @Override
            public void mark() {
                LOG.trace("Null meter (name={}) marked", name);
                // Do nothing
            }

            @Override
            public void mark(long n) {
                LOG.trace("Null meter (name={}) marked by {}", name, n);
                // Do nothing
            }

        };
    }

    @Override
    public Histogram histogram(final String name) {

        LOG.trace("Null histogram (name={}) registered", name);

        return new Histogram() {

            @Override
            public void update(long value) {
                LOG.trace("Null histogram (name={}) updated by {}", name, value);
                // Do nothing
            }

            @Override
            public void update(int value) {
                LOG.trace("Null histogram (name={}) updated by {}", name, value);
                // Do nothing
            }
        };
    }

    /* ********************************************** Private methods *********************************************** */

}