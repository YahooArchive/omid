package com.yahoo.omid.committable.hbase;

import com.google.inject.AbstractModule;
import com.yahoo.omid.committable.CommitTable;

import javax.inject.Singleton;

public class HBaseCommitTableStorageModule extends AbstractModule {

    @Override
    public void configure() {

        // HBase commit table creation
        bind(CommitTable.class).to(HBaseCommitTable.class).in(Singleton.class);

    }

}
