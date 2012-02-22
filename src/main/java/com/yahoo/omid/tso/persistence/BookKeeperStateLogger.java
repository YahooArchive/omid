package com.yahoo.omid.tso.persistence;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.zookeeper.ZooKeeper;

import com.yahoo.omid.tso.LedgerHandle;

public class BookKeeperStateLogger implements StateLogger {
    
    private BookKeeper bookkeeper;
    private LedgerHandle lh;
    
    @Override
    public void initialize() {
        // TODO Auto-generated method stub

    }

    @Override
    public void addRecord() {
        // TODO Auto-generated method stub

    }

}
