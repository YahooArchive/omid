package com.yahoo.omid.timestamp.storage;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;

public class ZKTimestampStorageModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(ZKTimestampStorageModule.class);

    @Override
    public void configure() {
        // Timestamp storage creation
        bind(TimestampStorage.class).to(ZKTimestampStorage.class).in(Singleton.class);

    }

}
