package com.yahoo.omid.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Optional;

public class MetricsRegistryMap {

    private final ConcurrentMap<String, Metric> metrics = new ConcurrentHashMap<>();

    interface MetricBuilder<T extends Metric> {

        MetricBuilder<Gauge<? extends Number>> GAUGES = new MetricBuilder<Gauge<? extends Number>>() {
            @Override
            public boolean isInstance(Metric metric) {
                return Gauge.class.isInstance(metric);
            }
        };

        MetricBuilder<Counter> COUNTERS = new MetricBuilder<Counter>() {
            @Override
            public boolean isInstance(Metric metric) {
                return Counter.class.isInstance(metric);
            }
        };

        MetricBuilder<Timer> TIMERS = new MetricBuilder<Timer>() {
            @Override
            public boolean isInstance(Metric metric) {
                return Timer.class.isInstance(metric);
            }
        };

        MetricBuilder<Meter> METERS = new MetricBuilder<Meter>() {
            @Override
            public boolean isInstance(Metric metric) {
                return Meter.class.isInstance(metric);
            }
        };

        MetricBuilder<Histogram> HISTOGRAMS = new MetricBuilder<Histogram>() {
            @Override
            public boolean isInstance(Metric metric) {
                return Histogram.class.isInstance(metric);
            }
        };

        boolean isInstance(Metric metric);
    }

    public Optional<? extends Metric> get(final String name,
                                          MetricBuilder<? extends Metric> builder,
                                          Class<? extends Metric> type) {

        final Metric metric = metrics.get(name);
        if (builder.isInstance(metric)) {
            return Optional.of(type.cast(metric));
        }
        return Optional.absent();

    }

    @SuppressWarnings("unchecked")
    public <T extends Metric, U extends Number> List<Gauge<U>> getGauges() {
        List<Gauge<U>> gaugesList = new ArrayList<>();
        for(Metric metric : metrics.values()) {
            if(metric instanceof Gauge) {
                gaugesList.add((Gauge<U>) metric);
            }
        }
        return gaugesList;
    }

    public void register(String name, Metric metric) throws IllegalArgumentException {
        final Metric existing = metrics.putIfAbsent(name, metric);
        if (existing != null) {
            throw new IllegalArgumentException("A metric named " +
                                               name +
                                               " of class " +
                                               metric.getClass().getCanonicalName() +
                                               " already exists");
        }
    }

}