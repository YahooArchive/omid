package com.yahoo.omid.committable.hbase;

import java.io.IOException;

/**
 * Implementations of this interface determine how keys are spread in HBase
 */
interface KeyGenerator {
    byte[] startTimestampToKey(long startTimestamp) throws IOException;
    long keyToStartTimestamp(byte[] key) throws IOException;
}
