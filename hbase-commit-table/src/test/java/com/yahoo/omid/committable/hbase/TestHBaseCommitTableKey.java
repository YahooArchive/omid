package com.yahoo.omid.committable.hbase;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.io.IOException;

import com.yahoo.omid.committable.hbase.HBaseCommitTable.KeyGenerator;

public class TestHBaseCommitTableKey {

    @Test
    public void testEncodeDecode() throws Exception {
        testKeyGen(new HBaseCommitTable.BucketKeyGenerator());
        testKeyGen(new HBaseCommitTable.BadRandomKeyGenerator());
        testKeyGen(new HBaseCommitTable.FullRandomKeyGenerator());
        testKeyGen(new HBaseCommitTable.SeqKeyGenerator());
    }

    @Test(enabled = false)
    private void testKeyGen(KeyGenerator keyGen) throws IOException {
        AssertJUnit.assertEquals("Should match", 0, keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(0)));
        AssertJUnit.assertEquals("Should match", 1, keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(1)));
        AssertJUnit.assertEquals("Should match", 8, keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(8)));
        AssertJUnit.assertEquals("Should match", 1024, keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(1024)));
        AssertJUnit.assertEquals("Should match", 1234, keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(1234)));
        AssertJUnit.assertEquals("Should match", 4321, keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(4321)));
        AssertJUnit.assertEquals("Should match", 0xdeadbeefcafeL,
                keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(0xdeadbeefcafeL)));
        AssertJUnit.assertEquals("Should match", Long.MAX_VALUE,
                keyGen.keyToStartTimestamp(keyGen.startTimestampToKey(Long.MAX_VALUE)));
    }

}
