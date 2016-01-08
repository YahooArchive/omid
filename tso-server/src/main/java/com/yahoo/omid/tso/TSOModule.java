package com.yahoo.omid.tso;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.yahoo.omid.timestamp.storage.TimestampStorage;

import javax.inject.Singleton;

import static com.yahoo.omid.committable.CommitTable.COMMIT_TABLE_NAME_KEY;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.TSO_BATCH_PERSIST_TIMEOUT_MS_KEY;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.TSO_MAX_BATCH_SIZE_KEY;
import static com.yahoo.omid.tso.RequestProcessorImpl.TSO_MAX_ITEMS_KEY;

class TSOModule extends AbstractModule {

    private final TSOServerCommandLineConfig config;

    TSOModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TSOChannelHandler.class).in(Singleton.class);

        bind(TSOStateManager.class).to(TSOStateManagerImpl.class).in(Singleton.class);

        bind(TimestampOracle.class).to(TimestampOracleImpl.class).in(Singleton.class);
        bind(Panicker.class).to(SystemExitPanicker.class).in(Singleton.class);

        bindConstant().annotatedWith(Names.named(TSO_MAX_BATCH_SIZE_KEY))
            .to(config.getMaxBatchSize());
        bindConstant().annotatedWith(Names.named(TSO_MAX_ITEMS_KEY))
            .to(config.getMaxItems());
        bindConstant().annotatedWith(Names.named(TSO_BATCH_PERSIST_TIMEOUT_MS_KEY))
            .to(config.getBatchPersistTimeoutMS());

        bindConstant().annotatedWith(Names.named(COMMIT_TABLE_NAME_KEY))
            .to(config.getCommitTable());

        bindConstant().annotatedWith(Names.named(TimestampStorage.TIMESTAMPSTORAGE_TABLE_NAME_KEY))
            .to(config.getTimestampTable());

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

}
