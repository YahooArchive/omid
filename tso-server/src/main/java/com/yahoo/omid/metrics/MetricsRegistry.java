package com.yahoo.omid.metrics;

public interface MetricsRegistry {

    /**
     * Registers the {@link Gauge} under the given name.
     *
     * @param name the name of the metric
     * @return a new {@link Counter}
     */
    public <T extends Number> void gauge(String name, Gauge<T> gauge);

    /**
     * Creates a new {@link Counter} and registers it under the given name.
     *
     * @param name the name of the metric
     * @return a new {@link Counter}
     */
    public Counter counter(String name);

    /**
     * Creates a new {@link Timer} and registers it under the given name.
     *
     * @param name the name of the metric
     * @return a new {@link Timer}
     */
    public Timer timer(String name);

    /**
     * Creates a new {@link Meter} and registers it under the given name.
     *
     * @param name the name of the metric
     * @return a new {@link Meter}
     */
    public Meter meter(String name);

    /**
     * Creates a new {@link Histogram} and registers it under the given name.
     *
     * @param name the name of the metric
     * @return a new {@link Histogram}
     */
    public Histogram histogram(String name);
}
