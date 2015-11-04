package com.yahoo.omid.tso;

import static com.yahoo.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

import java.io.IOException;

import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.tso.LeaseManagement.LeaseManagementException;

public class LeaseManagementModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseManagementModule.class);

    private final TSOServerCommandLineConfig config;

    public LeaseManagementModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    LeaseManagement provideLeaseManager(@Named(TSO_HOST_AND_PORT_KEY) String tsoHostAndPort,
                                                                   TSOChannelHandler tsoChannelHandler,
                                                                   TSOStateManager stateManager,
                                                                   CuratorFramework zkClient,
                                                                   Panicker panicker)
    throws LeaseManagementException {

        if (config.shouldHostAndPortBePublishedInZK) {
            LOG.info("Connection to ZK cluster [{}]", zkClient.getState());
            return new LeaseManager(tsoHostAndPort,
                                    tsoChannelHandler,
                                    stateManager,
                                    config.getLeasePeriodInMs(),
                                    zkClient,
                                    panicker);
        } else {
            return new NonHALeaseManager(tsoChannelHandler, stateManager);
        }

    }

}
