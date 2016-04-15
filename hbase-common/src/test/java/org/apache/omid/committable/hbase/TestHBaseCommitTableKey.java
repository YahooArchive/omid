/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.committable.hbase;

import org.apache.omid.committable.hbase.KeyGeneratorImplementations.BadRandomKeyGenerator;
import org.apache.omid.committable.hbase.KeyGeneratorImplementations.BucketKeyGenerator;
import org.apache.omid.committable.hbase.KeyGeneratorImplementations.FullRandomKeyGenerator;
import org.apache.omid.committable.hbase.KeyGeneratorImplementations.SeqKeyGenerator;
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
