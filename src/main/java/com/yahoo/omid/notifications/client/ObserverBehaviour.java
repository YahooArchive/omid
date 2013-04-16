package com.yahoo.omid.notifications.client;

import org.apache.hadoop.hbase.client.Result;

import com.yahoo.omid.client.TransactionState;

public interface ObserverBehaviour {
    /**
     * The observer will be notified through this method when a change is detected on its interest in the data store.
     * 
     * @param rowData
     *            the data of the particular row where the observer's interest that has changed belongs to
     * @param tx
     *            a transactional context that the observer can use to execute additional operations in the data store
     */
    public void onInterestChanged(Result rowData, TransactionState tx);
}
