package com.yahoo.omid.tso;

import javax.inject.Singleton;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.metrics.MetricsUtils;

public class TSOModule extends AbstractModule {

    private final TSOServerCommandLineConfig config;

    public TSOModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TimestampOracle.class).to(TimestampOracleImpl.class).in(Singleton.class);

        // Disruptor setup
        bind(RequestProcessor.class).to(RequestProcessorImpl.class).in(Singleton.class);
        bind(PersistenceProcessor.class).to(PersistenceProcessorImpl.class).in(Singleton.class);
        bind(ReplyProcessor.class).to(ReplyProcessorImpl.class).in(Singleton.class);
        bind(RetryProcessor.class).to(RetryProcessorImpl.class).in(Singleton.class);

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
