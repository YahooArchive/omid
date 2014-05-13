package com.yahoo.omid.committable.hbase;

import static org.junit.Assert.*;
import static com.yahoo.omid.committable.hbase.HBaseCommitTable.keyToStartTimestamp;
import static com.yahoo.omid.committable.hbase.HBaseCommitTable.startTimestampToKey;
import org.junit.Test;

public class TestHBaseCommitTableKey {

    @Test
    public void testEncodeDecode() {
        assertEquals("Should match", 0, keyToStartTimestamp(startTimestampToKey(0)));
        assertEquals("Should match", 1, keyToStartTimestamp(startTimestampToKey(1)));
        assertEquals("Should match", 8, keyToStartTimestamp(startTimestampToKey(8)));
        assertEquals("Should match", 1024, keyToStartTimestamp(startTimestampToKey(1024)));
        assertEquals("Should match", 1234, keyToStartTimestamp(startTimestampToKey(1234)));
        assertEquals("Should match", 4321, keyToStartTimestamp(startTimestampToKey(4321)));
        assertEquals("Should match", 0xdeadbeefcafeL, keyToStartTimestamp(startTimestampToKey(0xdeadbeefcafeL)));
        assertEquals("Should match", Long.MAX_VALUE, keyToStartTimestamp(startTimestampToKey(Long.MAX_VALUE)));
    }

}
