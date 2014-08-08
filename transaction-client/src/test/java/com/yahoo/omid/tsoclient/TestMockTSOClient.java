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

package com.yahoo.omid.tsoclient;

import static org.testng.AssertJUnit.assertEquals;
import org.testng.annotations.Test;
import org.testng.Assert;
import org.testng.AssertJUnit;
import java.util.concurrent.ExecutionException;

import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.InMemoryCommitTable;

import com.yahoo.omid.tso.util.DummyCellIdImpl;

import com.google.common.collect.Sets;
import com.yahoo.omid.tsoclient.TSOClient.AbortException;

public class TestMockTSOClient {

    final static public CellId c1 = new DummyCellIdImpl(0xdeadbeefL);
    final static public CellId c2 = new DummyCellIdImpl(-0xfeedcafeL);

    @Test(timeOut=10000)
    public void testConflicts() throws Exception {
        CommitTable commitTable = new InMemoryCommitTable();
        TSOClient client = new MockTSOClient(commitTable.getWriter().get());

        long tr1 = client.getNewStartTimestamp().get();
        long tr2 = client.getNewStartTimestamp().get();

        long cr1 = client.commit(tr1, Sets.newHashSet(c1)).get();

        try {
            long cr2 = client.commit(tr2, Sets.newHashSet(c1, c2)).get();
            Assert.fail("Shouldn't have committed");
        } catch (ExecutionException ee) {
            assertEquals("Should have aborted", ee.getCause().getClass(), AbortException.class);
        }
    }

    @Test(timeOut=10000)
    public void testWatermarkUpdate() throws Exception {
        CommitTable commitTable = new InMemoryCommitTable();
        TSOClient client = new MockTSOClient(commitTable.getWriter().get());
        CommitTable.Client commitTableClient = commitTable.getClient().get();

        long tr1 = client.getNewStartTimestamp().get();
        client.commit(tr1, Sets.newHashSet(c1)).get();

        long initWatermark = commitTableClient.readLowWatermark().get();

        long tr2 = client.getNewStartTimestamp().get();
        client.commit(tr2, Sets.newHashSet(c1)).get();

        long newWatermark = commitTableClient.readLowWatermark().get();
        AssertJUnit.assertTrue("new low watermark should be bigger", newWatermark > initWatermark);
    }
}
