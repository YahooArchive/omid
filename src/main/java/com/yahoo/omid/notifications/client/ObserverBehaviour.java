package com.yahoo.omid.notifications.client;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;

import com.yahoo.omid.transaction.Transaction;

public interface ObserverBehaviour {
    /**
     * The observer will be notified through this method when a change is detected on its interest in the data store.
     * 
     * @param rowData
     *            the data of the particular row where the observer's interest that has changed belongs to
     * @param tx
     *            a transactional context that the observer can use to execute additional operations in the data store
     * @throws IOException
     *             implementations might choose to ignore HBase IOExceptions and the transaction will be aborted
     *             automatically
     */
    public void onInterestChanged(Result rowData, Transaction tx) throws IOException;
}
