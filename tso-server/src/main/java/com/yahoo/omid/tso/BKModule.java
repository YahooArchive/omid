package com.yahoo.omid.tso;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class BKModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(BKModule.class);

    private final TSOServerCommandLineConfig config;

    public BKModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    public void configure() {
    }

    @Provides
    BookKeeper provideBookkeeperClient() throws Exception {

        String zkCluster = config.getZKCluster();

        LOG.info("Creating BooKkeeper Client connecting to {}", zkCluster);

        ClientConfiguration conf = new ClientConfiguration()
                                                            .setZkServers(zkCluster)
                                                            .setZkTimeout(300_000); // 5 minutes
        return new BookKeeper(conf);
    }

}
