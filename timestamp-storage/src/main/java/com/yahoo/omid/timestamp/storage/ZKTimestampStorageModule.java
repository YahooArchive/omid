package com.yahoo.omid.timestamp.storage;

import com.google.inject.AbstractModule;

import javax.inject.Singleton;

public class ZKTimestampStorageModule extends AbstractModule {


    @Override
    public void configure() {
        // Timestamp storage creation
        bind(TimestampStorage.class).to(ZKTimestampStorage.class).in(Singleton.class);

    }

}
