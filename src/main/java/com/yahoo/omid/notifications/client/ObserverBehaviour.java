package com.yahoo.omid.notifications.client;

public interface ObserverBehaviour {
    public void updated(byte[] table, byte[] rowKey, byte[] columnFamily, byte[] column);
}
