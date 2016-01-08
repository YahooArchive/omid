package com.yahoo.omid.metrics;

/**
 * Provider to provide metrics logger for different scopes.
 */
public interface MetricsProvider {

    String CODAHALE_METRICS_CONFIG = "console:_:60:SECONDS";

    enum Provider {
        CODAHALE, YMON;
    }

    /**
     * Intialize the metrics provider.
     */
    void startMetrics();

    /**
     * Close the metrics provider.
     */
    void stopMetrics();

}
