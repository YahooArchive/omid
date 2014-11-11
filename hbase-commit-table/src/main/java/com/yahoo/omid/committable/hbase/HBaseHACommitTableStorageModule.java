package com.yahoo.omid.committable.hbase;

import static com.yahoo.omid.ZKConstants.OMID_NAMESPACE;
import static com.yahoo.omid.committable.hbase.HBaseHACommitTable.HA_COMMIT_TABLE_KEY;
import java.io.IOException;

import javax.inject.Named;
import javax.inject.Singleton;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerSet.MetadataStorage;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.yahoo.omid.committable.CommitTable;

public class HBaseHACommitTableStorageModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseHACommitTableStorageModule.class);

    private final String zkCluster;

    public HBaseHACommitTableStorageModule(String zkCluster) {
        this.zkCluster = zkCluster;
    }

    @Override
    public void configure() {

        bind(CommitTable.class).to(HBaseCommitTable.class).in(Singleton.class);
        bind(MetadataStorage.class).annotatedWith(Names.named(HA_COMMIT_TABLE_KEY))
                                   .to(ZKBasedMetadataStorage.class);

    }

    @Provides
    @Named(HA_COMMIT_TABLE_KEY)
    CuratorFramework provideZookeeperClient() {

        LOG.info("Creating Zookeeper Client connecting to {}", zkCluster);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        return CuratorFrameworkFactory.builder()
                                      .namespace(OMID_NAMESPACE)
                                      .connectString(zkCluster)
                                      .retryPolicy(retryPolicy)
                                      .build();
    }

    @Provides
    @Named(HA_COMMIT_TABLE_KEY)
    BookKeeper provideBookKeeperClient() throws IOException, InterruptedException, KeeperException {

        LOG.info("Creating BK client connected to ZK cluster {}...", zkCluster);

        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkServers(zkCluster);
        return new BookKeeper(conf);

    }

}
