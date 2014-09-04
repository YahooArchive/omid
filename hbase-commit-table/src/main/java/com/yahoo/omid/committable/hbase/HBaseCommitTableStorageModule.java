package com.yahoo.omid.committable.hbase;

import javax.inject.Singleton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.committable.CommitTable;

public class HBaseCommitTableStorageModule extends AbstractModule {

    @Override
    public void configure() {

        // HBase commit table creation
        bind(CommitTable.class).to(HBaseCommitTable.class).in(Singleton.class);

    }

    @Provides
    Configuration provideHBaseConfiguration() {
        return HBaseConfiguration.create();
    }
}
