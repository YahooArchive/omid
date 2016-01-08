package com.yahoo.omid.timestamp.storage;

import com.google.inject.AbstractModule;

import javax.inject.Singleton;

public class HBaseTimestampStorageModule extends AbstractModule {

    @Override
    public void configure() {

        // Timestamp storage creation
        bind(TimestampStorage.class).to(HBaseTimestampStorage.class).in(Singleton.class);

    }

}
