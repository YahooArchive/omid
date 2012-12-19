package com.yahoo.omid.notifications.client;

import com.yahoo.omid.client.TransactionState;

public interface ObserverBehaviour {
    public void updated(TransactionState tx, byte[] table, byte[] rowKey, byte[] columnFamily, byte[] column);
}
