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
import com.yahoo.omid.tsoclient.TSOClient.AbortException;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

public class TestConflict extends TSOTestBase {
    @Test(timeout=10000)
    public void testConflict() throws Exception {
        long tr1 = client.createTransaction().get();
        long tr2 = client.createTransaction().get();
        assertTrue("second txn should have higher timestamp", tr2 > tr1);

        long cr1 = client.commit(tr1, Sets.newHashSet(c1)).get();
        assertTrue("commit ts must be higher than start ts", cr1 > tr1);

        try {
            long cr2 = client.commit(tr2, Sets.newHashSet(c1, c2)).get();
            fail("Second commit should fail");
        } catch (ExecutionException ee) {
            assertEquals("Should have aborted", ee.getCause().getClass(), AbortException.class);
        }
    }

}
