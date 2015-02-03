package com.yahoo.omid.tso;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class TestCommitQuery extends TSOTestBase {

    @Test(timeOut = 30000)
    public void testCommitQuery() throws Exception {
        long startTsForTx1 = client.getNewStartTimestamp().get();
        long startTsForTx2 = client.getNewStartTimestamp().get();
        AssertJUnit.assertTrue("Start timestamps should grow", startTsForTx2 > startTsForTx1);

        AssertJUnit.assertFalse("Commit ts for Tx1 shouldn't appear in commit table",
                                getCommitTableClient().getCommitTimestamp(startTsForTx1).get().isPresent());

        long commitTsForTx1 = client.commit(startTsForTx1, Sets.newHashSet(c1)).get();
        AssertJUnit.assertTrue("Commit timestamp should be higher than start timestamp for the same tx",
                               commitTsForTx1 > startTsForTx1);

        Long commitTs1InCommitTable = getCommitTableClient().getCommitTimestamp(startTsForTx1).get().get();
        AssertJUnit.assertNotNull("Tx is committed, should return as such from commit table", commitTs1InCommitTable);
        AssertJUnit.assertEquals("getCommitTimestamp() & commit() should report same commitTs value for same tx",
                                 (long) commitTs1InCommitTable, commitTsForTx1);
        AssertJUnit.assertTrue("Commit timestamp should be higher than previously created tx's start ts",
                               commitTs1InCommitTable > startTsForTx2);
    }
}
