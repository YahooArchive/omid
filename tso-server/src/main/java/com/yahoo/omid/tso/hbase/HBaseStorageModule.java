package com.yahoo.omid.tso.hbase;

import javax.inject.Singleton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.tso.TimestampOracleImpl.TimestampStorage;

public class HBaseStorageModule extends AbstractModule {

    @Override
    public void configure() {

        // HBase commit table creation
        bind(CommitTable.class).to(HBaseCommitTable.class).in(Singleton.class);

        // Timestamp storage creation
        bind(TimestampStorage.class).to(HBaseTimestampStorage.class).in(Singleton.class);

    }

    @Provides
    Configuration provideHBaseConfiguration() {
        return HBaseConfiguration.create();
    }
}
