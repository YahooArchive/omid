package com.yahoo.omid.metrics;

public interface Meter extends Metric {

    /**
     * Mark the occurrence of an event.
     */
    public void mark();

    /**
     * Mark the occurrence of a given number of events.
     *
     * @param n the number of events
     */
    public void mark(long n);

}
