package com.yahoo.omid.metrics;

/**
 * A gauge returns the value of a metric measured at a specific point in time.
 * For example the current size of. The value of T must be some numeric type.
 */
public interface Gauge<T extends Number> extends Metric {

    public T getValue();

}
