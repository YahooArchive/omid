package com.yahoo.omid.tso;

import static com.yahoo.omid.tso.RequestProcessorImpl.TSO_MAX_ITEMS_KEY;
import static com.yahoo.omid.tso.TSOServer.TSO_EPOCH_KEY;
import static com.yahoo.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;
import static org.mockito.Mockito.mock;

import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.curator.framework.CuratorFramework;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.InMemoryCommitTable;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import com.yahoo.omid.timestamp.storage.TimestampStorage;
import com.yahoo.omid.tso.TSOServer.LeaseManager;
import com.yahoo.omid.tso.TimestampOracleImpl.InMemoryTimestampStorage;

public class TSOMockModule extends AbstractModule {

    private final TSOServerCommandLineConfig config;

    public TSOMockModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(CommitTable.class).to(InMemoryCommitTable.class).in(Singleton.class);
        bind(TimestampStorage.class).to(InMemoryTimestampStorage.class).in(Singleton.class);
        bind(TimestampOracle.class).to(PausableTimestampOracle.class).in(Singleton.class);
        bind(Panicker.class).to(MockPanicker.class).in(Singleton.class);

        // Disruptor setup
        // Overwrite default value
        bindConstant().annotatedWith(Names.named(TSO_MAX_ITEMS_KEY)).to(config.getMaxItems());
        install(new DisruptorModule());

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
