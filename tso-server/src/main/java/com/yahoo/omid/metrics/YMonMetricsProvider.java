package com.yahoo.omid.metrics;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ymonsb_java.yms.mon_metrics.MonMetrics;
import ymonsb_java.yms.mon_metrics.MonMetricsException;

import com.google.common.util.concurrent.AbstractScheduledService;

public class YMonMetricsProvider extends AbstractScheduledService implements MetricsProvider, MetricsRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(YMonMetricsProvider.class);

    private final MonMetrics metrics;
    private final int outputFreqInSec;

    private final MetricsRegistryMap metricsMap = new MetricsRegistryMap();

    @Inject
    public YMonMetricsProvider(YMonMetricsConfig conf) throws MonMetricsException {
        metrics = new MonMetrics(conf.getPrefix(), MonMetrics.SHM);
        outputFreqInSec = conf.getOutputFreq();
    }

    /* ********************************** AbstractScheduledService implementation ********************************** */

    @Override
    protected void runOneIteration() throws Exception {
        LOG.debug("Sending metrics data...");
        List<Gauge<Number>> gauges = metricsMap.getGauges();
        for(Gauge<Number> gauge : gauges) {
            metrics.set(((YMonGaugeWrapper<Number>) gauge).getName(), gauge.getValue().doubleValue(), MonMetrics.NUMBER);
        }
        metrics.send();
    }

    //two methods because of guava 11
    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(outputFreqInSec, outputFreqInSec, TimeUnit.SECONDS);
    }

    /* ********************************** MetricsProvider interface implementation ********************************** */

    @Override
    public void startMetrics() {
        startAndWait();
        LOG.info("Yahoo metrics provider started");
    }

    @Override
    public void stopMetrics() {
        stopAndWait();
        LOG.info("Yahoo metrics provider stopped");
    }

    /* ********************************** MetricsRegistry interface implementation ********************************** */

    private class YMonGaugeWrapper<T extends Number> implements Gauge<T> {

        private final String name;
        private final Gauge<T> appGauge;

        public YMonGaugeWrapper(String name, Gauge<T> omidGauge) {
            this.name = name;
            this.appGauge = omidGauge;
        }

        public String getName() {
            return name;
        }
        @Override
        public T getValue() {
            return appGauge.getValue();
        }

    }

    @Override
    public <T extends Number> void gauge(String name, Gauge<T> gauge) {
        Metric counterGauge = new YMonGaugeWrapper<T>(name, gauge);
        metricsMap.register(name, counterGauge);
    }

    @Override
    public Counter counter(final String name) {

        return new Counter() {

            @Override
            public void inc() {
                try {
                    metrics.increment(name, 1, MonMetrics.NUMBER | MonMetrics.RATE | MonMetrics.AGG_MASK);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void inc(long n) {
                try {
                    metrics.increment(name, n, MonMetrics.NUMBER | MonMetrics.SUM | MonMetrics.AGG_MASK);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            @Override
            public void dec() {
                try {
                    metrics.increment(name, -1, MonMetrics.NUMBER | MonMetrics.SUM | MonMetrics.AGG_MASK);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            @Override
            public void dec(long n) {
                try {
                    metrics.increment(name, n * -1, MonMetrics.NUMBER | MonMetrics.SUM | MonMetrics.AGG_MASK);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

        };

    }

    private class YMonTimerWrapper implements Timer {

        private final Meter meter;
        private final Histogram histogram;

        private long startTimeInNs;

        public YMonTimerWrapper(String name) {
            meter = meter(name + ":rate");
            histogram = histogram(name + ":latency");
        }

        @Override
        public void start() {
            startTimeInNs = System.nanoTime();
        }

        @Override
        public void stop() {
            update(System.nanoTime() - startTimeInNs);
        }

        @Override
        public void update(long durationInNs) {
            if (durationInNs >= 0) {
                histogram.update(durationInNs);
                meter.mark();
            }
        }

    }

    @Override
    public Timer timer(final String name) {

        return new YMonTimerWrapper(name);

    }

    @Override
    public Meter meter(final String name) {

        return new Meter() {

            @Override
            public void mark() {
                try {
                    metrics.increment(name, 1, MonMetrics.NUMBER | MonMetrics.RATE);
                } catch (MonMetricsException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void mark(long n) {
                try {
                    metrics.increment(name, n, MonMetrics.NUMBER | MonMetrics.RATE);
                } catch (MonMetricsException e) {
                    e.printStackTrace();
                }
            }

        };
    }

    @Override
    public Histogram histogram(final String name) {

        try {
            metrics.defineHistogram(name, MonMetrics.HISTOGRAM, new double[] { 0, 0.00001, 0.0001, 0.001, 0.01,
                    0.1, 0.5, 1, 2, 10, 100 });
        } catch (MonMetricsException e1) {
            e1.printStackTrace();
        }

        return new Histogram() {

            @Override
            public void update(long value) {

                try {
                    metrics.set(name, value, MonMetrics.HISTOGRAM);
                } catch (MonMetricsException e) {
                    e.printStackTrace();
                }

            }

            @Override
            public void update(int value) {

                try {
                    metrics.set(name, value, MonMetrics.HISTOGRAM);
                } catch (MonMetricsException e) {
                    e.printStackTrace();
                }

            }
        };
    }

    /* ********************************************** Private methods *********************************************** */

}