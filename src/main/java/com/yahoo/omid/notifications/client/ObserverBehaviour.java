package com.yahoo.omid.notifications.client;

import com.yahoo.omid.client.TransactionState;

public interface ObserverBehaviour {
    /**
     * Client applications will be notified through this method when a column change is detected on the data store, 
     * so they can implement the behavior of each observer they contain.
     * 
     * @param column the column changed in the data store
     * @param columnFamily the column family where the column changed belongs to
     * @param table the table where the column changed belongs to
     * @param rowKey the rowkey where the column was changed
     * @param tx a transactional context that the observer can use to execute additional operations in the data store
     */
    public void onColumnChanged(byte[] column, byte[] columnFamily, byte[] table, byte[] rowKey, TransactionState tx);
}
