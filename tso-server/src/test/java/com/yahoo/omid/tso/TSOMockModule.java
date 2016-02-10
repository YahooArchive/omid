package com.yahoo.omid.tso;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.InMemoryCommitTable;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import com.yahoo.omid.timestamp.storage.TimestampStorage;
import com.yahoo.omid.tso.TimestampOracleImpl.InMemoryTimestampStorage;

public class TSOMockModule extends AbstractModule {

    private final TSOServerCommandLineConfig config;

    public TSOMockModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TSOChannelHandler.class).in(Singleton.class);

        bind(TSOStateManager.class).to(TSOStateManagerImpl.class).in(Singleton.class);

        bind(CommitTable.class).to(InMemoryCommitTable.class).in(Singleton.class);
        bind(TimestampStorage.class).to(InMemoryTimestampStorage.class).in(Singleton.class);
        bind(TimestampOracle.class).to(PausableTimestampOracle.class).in(Singleton.class);
        bind(Panicker.class).to(MockPanicker.class).in(Singleton.class);

        // Disruptor setup
        install(new DisruptorModule());

        // LeaseManagement setup
        install(new LeaseManagementModule(config));

        // ZK Module
        install(new ZKModule(config));

    }

    @Provides
    TSOServerCommandLineConfig provideTSOServerConfig() {
        return config;
    }

    @Provides @Singleton
    MetricsRegistry provideMetricsRegistry() {
        return new NullMetricsProvider();
    }

}
