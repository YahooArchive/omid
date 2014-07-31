package com.yahoo.omid.tso;

import static com.yahoo.omid.committable.hbase.HBaseCommitTable.HBASE_COMMIT_TABLE_NAME_KEY;
import static com.yahoo.omid.tso.hbase.HBaseTimestampStorage.HBASE_TIMESTAMPSTORAGE_TABLE_NAME_KEY;

import javax.inject.Singleton;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.yahoo.omid.metrics.MetricsUtils;

public class TSOModule extends AbstractModule {

    private final TSOServerCommandLineConfig config;

    public TSOModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TimestampOracle.class).to(TimestampOracleImpl.class).in(Singleton.class);
        bind(Panicker.class).to(SystemExitPanicker.class).in(Singleton.class);

        bindConstant().annotatedWith(Names.named(HBASE_COMMIT_TABLE_NAME_KEY))
                .to(config.getHBaseCommitTable());

        bindConstant().annotatedWith(Names.named(HBASE_TIMESTAMPSTORAGE_TABLE_NAME_KEY))
                .to(config.getHBaseTimestampTable());

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
