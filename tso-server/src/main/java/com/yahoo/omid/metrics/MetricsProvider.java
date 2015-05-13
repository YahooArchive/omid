package com.yahoo.omid.metrics;


/**
 * Provider to provide metrics logger for different scopes.
 */
public interface MetricsProvider {

    public enum Provider {
        CODAHALE;
    }

    /**
     * Intialize the metrics provider.
     */
    public void startMetrics();

    /**
     * Close the metrics provider.
     */
    public void stopMetrics();

}
