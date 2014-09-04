package com.yahoo.omid.tso.hbase;

import javax.inject.Singleton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.timestamp.storage.TimestampStorage;

public class HBaseTimestampStorageModule extends AbstractModule {

    @Override
    public void configure() {

        // Timestamp storage creation
        bind(TimestampStorage.class).to(HBaseTimestampStorage.class).in(Singleton.class);

    }

    @Provides
    Configuration provideHBaseConfiguration() {
        return HBaseConfiguration.create();
    }

}
