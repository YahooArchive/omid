package com.yahoo.omid.tso;

import static com.yahoo.omid.committable.hbase.HBaseCommitTable.HBASE_COMMIT_TABLE_NAME_KEY;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.TSO_BATCH_PERSIST_TIMEOUT_MS_KEY;
import static com.yahoo.omid.tso.PersistenceProcessorImpl.TSO_MAX_BATCH_SIZE_KEY;
import static com.yahoo.omid.tso.RequestProcessorImpl.TSO_MAX_ITEMS_KEY;
import static com.yahoo.omid.tso.TSOServer.TSO_EPOCH_KEY;
import static com.yahoo.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;
import static com.yahoo.omid.tso.hbase.HBaseTimestampStorage.HBASE_TIMESTAMPSTORAGE_TABLE_NAME_KEY;

import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.curator.framework.CuratorFramework;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.yahoo.omid.tso.TSOServer.LeaseManager;

public class TSOModule extends AbstractModule {

    private final TSOServerCommandLineConfig config;

    public TSOModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TimestampOracle.class).to(TimestampOracleImpl.class).in(Singleton.class);
        bind(Panicker.class).to(SystemExitPanicker.class).in(Singleton.class);

        bindConstant().annotatedWith(Names.named(TSO_MAX_BATCH_SIZE_KEY))
                .to(config.getMaxBatchSize());
        bindConstant().annotatedWith(Names.named(TSO_MAX_ITEMS_KEY))
                .to(config.getMaxItems());
        bindConstant().annotatedWith(Names.named(TSO_BATCH_PERSIST_TIMEOUT_MS_KEY))
            .to(config.getBatchPersistTimeoutMS());

        bindConstant().annotatedWith(Names.named(HBASE_COMMIT_TABLE_NAME_KEY))
                .to(config.getHBaseCommitTable());

        bindConstant().annotatedWith(Names.named(HBASE_TIMESTAMPSTORAGE_TABLE_NAME_KEY))
                .to(config.getHBaseTimestampTable());

        // Disruptor setup
        install(new DisruptorModule());

        // ZK Module
        install(new ZKModule(config));
    }

    @Provides
    TSOServerCommandLineConfig provideTSOServerConfig() {
        return config;
    }

    @Provides
    @Named(TSO_EPOCH_KEY)
    long provideEpoch(TimestampOracle timestampOracle) {
        return timestampOracle.getLast();
    }

    @Provides
    TSOServer.LeaseManager provideLeaseManager(@Named(TSO_HOST_AND_PORT_KEY) String tsoHostAndPort,
                                               @Named(TSO_EPOCH_KEY) long epoch,
                                               CuratorFramework zkClient)
    throws Exception {
        return new LeaseManager(tsoHostAndPort, epoch, config.getLeasePeriodInMs(), zkClient);
    }

}
