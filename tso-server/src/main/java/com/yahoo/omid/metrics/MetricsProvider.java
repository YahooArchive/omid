package com.yahoo.omid.metrics;

import java.util.regex.Pattern;

/**
 * Provider to provide metrics logger for different scopes.
 */
public interface MetricsProvider {

    public enum Provider {
        CODAHALE, YMON;
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
