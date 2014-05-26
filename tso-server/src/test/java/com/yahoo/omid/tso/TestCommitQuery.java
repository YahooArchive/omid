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

import org.junit.Test;

import com.google.common.collect.Sets;


public class TestCommitQuery extends TSOTestBase {

    @Test(timeout=10000)
    public void testCommitQuery() throws Exception {
        long tr1 = client.createTransaction().get();
        long tr2 = client.createTransaction().get();
        assertTrue("start timestamps should grow", tr2 > tr1);

        assertFalse("tr1 isn't committed",
                    getCommitTableClient().getCommitTimestamp(tr1).get().isPresent());

        long cr1 = client.commit(tr1, Sets.newHashSet(c1)).get();
        assertTrue("commit timestamp should be higher than start timestamp", cr1 > tr1);

        Long cq2 = getCommitTableClient().getCommitTimestamp(tr1).get().get();
        assertNotNull("transaction is committed, should return as such", cq2);
        assertEquals("getCommitTimestamp and commit should report same thing for same transaction",
                     (long)cq2, (long)cr1);
        assertTrue("commit should be higher than previously created transaction", cq2 > tr2);
    }
}
