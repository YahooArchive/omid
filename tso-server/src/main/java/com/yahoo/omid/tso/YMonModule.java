package com.yahoo.omid.tso;

import javax.inject.Singleton;

import ymonsb_java.yms.mon_metrics.MonMetricsException;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.YMonMetricsConfig;
import com.yahoo.omid.metrics.YMonMetricsProvider;

public class YMonModule extends AbstractModule {

    private final YMonMetricsConfig config;

    public YMonModule(YMonMetricsConfig config) {
        this.config = config;
    }

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
