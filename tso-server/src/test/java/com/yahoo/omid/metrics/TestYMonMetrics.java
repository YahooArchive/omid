package com.yahoo.omid.metrics;

import java.util.Random;

public class TestYMonMetrics {

    private static final int COUNTER = 3;

    private static final int OUTPUT_FREQ_IN_SECS = 1;

    private static YMonMetricsProvider metricsProvider;

    public static void main(String[] args) throws Exception {
        YMonMetricsConfig conf = new YMonMetricsConfig();
        conf.setOutputFreq(OUTPUT_FREQ_IN_SECS);
        metricsProvider = new YMonMetricsProvider(conf);
        metricsProvider.startMetrics();

        // Test counter
        Counter counter = metricsProvider.counter("testCounter");
        for (int i = 0; i < COUNTER; i++) {
            counter.inc();
        }
        counter.inc(COUNTER);

        // Test timer
        Timer timer = metricsProvider.timer("testTimer");
        timer.start();
        Thread.currentThread().sleep(1000);
        timer.stop();

        // Test meter
        Meter meter = metricsProvider.meter("testMeter");
        meter.mark();
        meter.mark(COUNTER);

        // Test histogram
        Histogram histogram = metricsProvider.histogram("testHistogram");
        Random r = new Random(System.currentTimeMillis());
        for (int i = 0; i < COUNTER; i++)
            histogram.update(r.nextInt(100));
        histogram.update(Long.MAX_VALUE);

        System.out.println("Waiting " + OUTPUT_FREQ_IN_SECS + " secs before stopping metrics");
        Thread.currentThread().sleep(1000 * OUTPUT_FREQ_IN_SECS);
        metricsProvider.stopMetrics();
        System.out.println("Metrics stopped");

    }

}