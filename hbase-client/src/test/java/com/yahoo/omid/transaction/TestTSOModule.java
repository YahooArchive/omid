package com.yahoo.omid.transaction;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.metrics.NullMetricsProvider;
import com.yahoo.omid.timestamp.storage.HBaseTimestampStorage;
import com.yahoo.omid.timestamp.storage.TimestampStorage;
import com.yahoo.omid.tso.DisruptorModule;
import com.yahoo.omid.tso.LeaseManagement;
import com.yahoo.omid.tso.MockPanicker;
import com.yahoo.omid.tso.Panicker;
import com.yahoo.omid.tso.PausableLeaseManager;
import com.yahoo.omid.tso.PausableTimestampOracle;
import com.yahoo.omid.tso.TSOChannelHandler;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import com.yahoo.omid.tso.TSOStateManager;
import com.yahoo.omid.tso.TSOStateManagerImpl;
import com.yahoo.omid.tso.TimestampOracle;
import com.yahoo.omid.tso.ZKModule;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;

import static com.yahoo.omid.ZKConstants.CURRENT_TSO_PATH;
import static com.yahoo.omid.ZKConstants.TSO_LEASE_PATH;
import static com.yahoo.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

/**
 * @author fperez@
 */
class TestTSOModule extends AbstractModule {
    private static final Logger LOG = LoggerFactory.getLogger(TestTSOModule.class);
    private final Configuration hBaseConfig;
    private final TSOServerCommandLineConfig config;

    TestTSOModule(Configuration hBaseConfig, TSOServerCommandLineConfig config) {
        this.hBaseConfig = hBaseConfig;
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TSOChannelHandler.class).in(Singleton.class);

        bind(TSOStateManager.class).to(TSOStateManagerImpl.class).in(Singleton.class);

        bind(CommitTable.class).to(HBaseCommitTable.class).in(Singleton.class);
        bind(TimestampStorage.class).to(HBaseTimestampStorage.class).in(Singleton.class);
        bind(TimestampOracle.class).to(PausableTimestampOracle.class).in(Singleton.class);
        bind(Panicker.class).to(MockPanicker.class).in(Singleton.class);

        // Disruptor setup
        install(new DisruptorModule());

        // ZK Module
        install(new ZKModule(config));

    }

    // LeaseManagement setup
    @Provides
    @Singleton
    LeaseManagement provideLeaseManager(@Named(TSO_HOST_AND_PORT_KEY) String tsoHostAndPort,
                                        TSOChannelHandler tsoChannelHandler,
                                        TSOStateManager stateManager,
                                        CuratorFramework zkClient,
                                        Panicker panicker)
        throws LeaseManagement.LeaseManagementException {

        LOG.info("Connection to ZK cluster [{}]", zkClient.getState());
        return new PausableLeaseManager(tsoHostAndPort,
                                        tsoChannelHandler,
                                        stateManager,
                                        config.getLeasePeriodInMs(),
                                        TSO_LEASE_PATH,
                                        CURRENT_TSO_PATH,
                                        zkClient,
                                        panicker);
    }

    @Provides
    Configuration provideHBaseConfig() {
        return hBaseConfig;
    }

    @Provides
    TSOServerCommandLineConfig provideTSOServerConfig() {
        return config;
    }

    @Provides
    @Singleton
    MetricsRegistry provideMetricsRegistry() {
        return new NullMetricsProvider();
    }

}
