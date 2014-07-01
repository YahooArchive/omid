package com.yahoo.omid.tso;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.NullCommitTable;
import com.yahoo.omid.tso.TimestampOracleImpl.InMemoryTimestampStorage;
import com.yahoo.omid.tso.TimestampOracleImpl.TimestampStorage;

public class InMemoryStorageModule extends AbstractModule {

    @Override
    public void configure() {

        bind(CommitTable.class).to(NullCommitTable.class).in(Singleton.class);
        bind(TimestampStorage.class).to(InMemoryTimestampStorage.class).in(Singleton.class);

    }

}
