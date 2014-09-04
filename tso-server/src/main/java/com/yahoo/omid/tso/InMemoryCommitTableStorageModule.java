package com.yahoo.omid.tso;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.NullCommitTable;

public class InMemoryCommitTableStorageModule extends AbstractModule {

    @Override
    public void configure() {

        bind(CommitTable.class).to(NullCommitTable.class).in(Singleton.class);

    }

}
