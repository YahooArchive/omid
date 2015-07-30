package com.yahoo.omid.tso;

import static com.yahoo.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

import java.util.concurrent.TimeUnit;

import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.tso.LeaseManagement.LeaseManagementException;

public class LeaseManagementModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(ZKModule.class);

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
                                                                   RequestProcessor requestProcessor,
                                                                   CuratorFramework zkClient)
    throws LeaseManagementException {

        if (config.shouldHostAndPortBePublishedInZK) {
            try {
                LOG.info("Connecting to ZK cluster [{}]", zkClient.getState());
                zkClient.start();
                if (zkClient.blockUntilConnected(10, TimeUnit.SECONDS)) {
                    LOG.info("Connection to ZK cluster [{}]", zkClient.getState());
                    return new LeaseManager(tsoHostAndPort,
                                            requestProcessor,
                                            config.getLeasePeriodInMs(),
                                            zkClient);
                } else {
                    throw new LeaseManagementException(
                            "Error creating LeaseManager. Can't contact ZK after 10 seconds");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LeaseManagementException("Interrupted whilst creating LeaseManager");
            }
        } else {
            return new NonHALeaseManager();
        }

    }

}
