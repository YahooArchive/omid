package com.yahoo.omid.tso;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.yahoo.omid.timestamp.storage.TimestampStorage;
import com.yahoo.omid.tso.TimestampOracleImpl.InMemoryTimestampStorage;

public class InMemoryTimestampStorageModule extends AbstractModule {

    @Override
    public void configure() {

        bind(TimestampStorage.class).to(InMemoryTimestampStorage.class).in(Singleton.class);

    }

}
