package com.yahoo.omid.tso;

import javax.inject.Singleton;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.metrics.MetricsUtils;
import com.yahoo.omid.tso.DisruptorModule;

public class TSOModule extends AbstractModule {

    private final TSOServerCommandLineConfig config;

    public TSOModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TimestampOracle.class).to(TimestampOracleImpl.class).in(Singleton.class);
        bind(Panicker.class).to(SystemExitPanicker.class).in(Singleton.class);

        // Disruptor setup
        install(new DisruptorModule());

    }

    @Provides
    TSOServerCommandLineConfig provideTSOServerConfig() {
        return config;
    }

    @Provides @Singleton
    MetricRegistry provideMetricsRegistry() {
        return MetricsUtils.initMetrics(config.getMetrics());
    }

}
