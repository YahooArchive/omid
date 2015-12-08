package com.yahoo.omid.metrics;

public interface Histogram extends Metric {

    /**
     * Adds a recorded value.
     *
     * @param value the length of the value
     */
    public void update(int value);

    /**
     * Adds a recorded value.
     *
     * @param value the length of the value
     */
    public void update(long value);

}
