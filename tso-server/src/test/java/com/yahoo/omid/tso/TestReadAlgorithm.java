package com.yahoo.omid.tso;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.testng.AssertJUnit;
import com.google.common.collect.Sets;
import com.yahoo.omid.tsoclient.TSOClient.AbortException;

import java.util.concurrent.ExecutionException;


public class TestReadAlgorithm extends TSOTestBase {
   
    @Test(timeOut=10000)
    public void testReadAlgorithm() throws Exception {      
        long tr1 = client.getNewStartTimestamp().get();
        long tr2 = client.getNewStartTimestamp().get();
        long tr3 = client.getNewStartTimestamp().get();

        long cr1 = client.commit(tr1, Sets.newHashSet(c1)).get();
        try {
            long cr2 = client.commit(tr3, Sets.newHashSet(c1, c2)).get();
            Assert.fail("Second commit should fail");
        } catch (ExecutionException ee) {
            AssertJUnit.assertEquals("Should have aborted", ee.getCause().getClass(), AbortException.class);
        }
        long tr4 = client2.getNewStartTimestamp().get();

        AssertJUnit.assertFalse("tr3 didn't commit",
                    getCommitTableClient().getCommitTimestamp(tr3).get().isPresent());
        AssertJUnit.assertTrue("txn committed after start timestamp",
                   (long) getCommitTableClient().getCommitTimestamp(tr1).get().get() > tr2);
        AssertJUnit.assertTrue("txn committed before start timestamp",
                   (long) getCommitTableClient().getCommitTimestamp(tr1).get().get() < tr4);
   }
   
}
