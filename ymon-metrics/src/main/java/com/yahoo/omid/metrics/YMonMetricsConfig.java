package com.yahoo.omid.metrics;

import com.google.inject.Inject;

import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
public class YMonMetricsConfig extends AbstractMetricsConfig {

    private static final String DEFAULT_PREFIX = "omid";

    private static final String METRICS_YAHOO_PREFIX_KEY = "metrics.yahoo.prefix";

    private String prefix = DEFAULT_PREFIX;

    public String getPrefix() {
        return prefix;
    }

    @Inject(optional = true)
    public void setPrefix(@Named(METRICS_YAHOO_PREFIX_KEY) String prefix) {
        this.prefix = prefix;
    }

}