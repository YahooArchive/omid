package com.yahoo.omid.tso;

import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.yahoo.omid.tsoclient.CellId;

// TODO: I think we should remove this test. This functionality is already tested in TestCommitQuery
public class TestCommit extends TSOTestBase {

    @Test(timeOut = 30000)
    public void testCommit() throws Exception {
        Long startTsTx1 = client.getNewStartTimestamp().get();
        Long commitTsTx1 = client.commit(startTsTx1, Sets.<CellId>newHashSet()).get();
        assertTrue(commitTsTx1 > startTsTx1);
   }

}
