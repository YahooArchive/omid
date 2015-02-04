package com.yahoo.omid.tso;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.testng.AssertJUnit;
import com.google.common.collect.Sets;
import com.yahoo.omid.tsoclient.TSOClient.AbortException;

import java.util.concurrent.ExecutionException;

public class TestConflict extends TSOTestBase {

    @Test(timeOut = 30000)
    public void testTwoConcurrentTxWithOverlappingWritesetsHaveConflicts() throws Exception {
        long startTsTx1 = client.getNewStartTimestamp().get();
        long startTsTx2 = client.getNewStartTimestamp().get();
        AssertJUnit.assertTrue("Second TX should have higher timestamp", startTsTx2 > startTsTx1);

        long commitTsTx1 = client.commit(startTsTx1, Sets.newHashSet(c1)).get();
        AssertJUnit.assertTrue("Commit timestamp must be higher than start ts for the same tx",
                               commitTsTx1 > startTsTx1);

        try {
            client.commit(startTsTx2, Sets.newHashSet(c1, c2)).get();
            Assert.fail("Second TX sould fail on commit");
        } catch (ExecutionException ee) {
            AssertJUnit.assertEquals("Should have aborted", ee.getCause().getClass(), AbortException.class);
        }
    }

}
