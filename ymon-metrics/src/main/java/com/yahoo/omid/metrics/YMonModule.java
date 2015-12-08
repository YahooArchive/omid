package com.yahoo.omid.metrics;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import ymonsb_java.yms.mon_metrics.MonMetricsException;

import javax.inject.Singleton;
import java.util.List;

public class YMonModule extends AbstractModule {

    private final YMonMetricsConfig config;

    public YMonModule(List<String> metricsConfigs) { this.config = new YMonMetricsConfig(); }

    @Override
    protected void configure() {

    }

    @Provides @Singleton
    MetricsRegistry provideMetricsRegistry() throws MonMetricsException {
        YMonMetricsProvider provider = new YMonMetricsProvider(config);
        provider.startMetrics();
        return provider;
    }

}
