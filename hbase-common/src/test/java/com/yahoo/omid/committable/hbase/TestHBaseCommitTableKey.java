package com.yahoo.omid.committable.hbase;

import com.yahoo.omid.committable.hbase.KeyGeneratorImplementations.BadRandomKeyGenerator;
import com.yahoo.omid.committable.hbase.KeyGeneratorImplementations.BucketKeyGenerator;
import com.yahoo.omid.committable.hbase.KeyGeneratorImplementations.FullRandomKeyGenerator;
import com.yahoo.omid.committable.hbase.KeyGeneratorImplementations.SeqKeyGenerator;
import org.testng.annotations.Test;

import java.io.IOException;

import static java.lang.Long.MAX_VALUE;
import static org.testng.Assert.assertEquals;

public class TestHBaseCommitTableKey {

    @Test
    public void testEncodeDecode() throws Exception {
        testKeyGen(new BucketKeyGenerator());
        testKeyGen(new BadRandomKeyGenerator());
        testKeyGen(new FullRandomKeyGenerator());
        testKeyGen(new SeqKeyGenerator());
    }

    @Test(enabled = false)
    private void testKeyGen(KeyGenerator keyGen) throws IOException {
        assertEquals(keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(0)), 0, "Should match");
        assertEquals(keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(1)), 1, "Should match");
        assertEquals(keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(8)), 8, "Should match");
        assertEquals(keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(1024)), 1024, "Should match");
        assertEquals(keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(1234)), 1234, "Should match");
        assertEquals(keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(4321)), 4321, "Should match");
        assertEquals(keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(0xdeadbeefcafeL)), 0xdeadbeefcafeL, "Should match");
        assertEquals(keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(MAX_VALUE)), MAX_VALUE, "Should match");
    }

}
