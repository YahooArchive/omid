/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso;

import static org.junit.Assert.*;

import com.google.common.collect.Sets;
import com.yahoo.omid.client.TSOClient.AbortException;

import java.util.concurrent.ExecutionException;

import org.junit.Test;


public class TestReadAlgorithm extends TSOTestBase {
   
    @Test(timeout=10000)
    public void testReadAlgorithm() throws Exception {      
        long tr1 = client.createTransaction().get();
        long tr2 = client.createTransaction().get();
        long tr3 = client.createTransaction().get();

        long cr1 = client.commit(tr1, Sets.newHashSet(c1)).get();
        try {
            long cr2 = client.commit(tr3, Sets.newHashSet(c1, c2)).get();
            fail("Second commit should fail");
        } catch (ExecutionException ee) {
            assertEquals("Should have aborted", ee.getCause().getClass(), AbortException.class);
        }
        long tr4 = client2.createTransaction().get();

        assertFalse("tr3 didn't commit",
                    getCommitTableClient().getCommitTimestamp(tr3).get().isPresent());
        assertTrue("txn committed after start timestamp",
                   (long) getCommitTableClient().getCommitTimestamp(tr1).get().get() > tr2);
        assertTrue("txn committed before start timestamp",
                   (long) getCommitTableClient().getCommitTimestamp(tr1).get().get() < tr4);
   }
   
}
