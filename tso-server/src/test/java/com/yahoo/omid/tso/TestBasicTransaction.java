package com.yahoo.omid.tso;

import static org.testng.AssertJUnit.assertEquals;
import org.testng.annotations.Test;
import org.testng.Assert;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Sets;
import com.yahoo.omid.tsoclient.TSOClient.AbortException;

// TODO: This test is effectively the same as TestConflict. Clean these tests in a
// future commit, removing duplicated code
public class TestBasicTransaction extends TSOTestBase {

    @Test(timeOut = 30000)
    public void testConflicts() throws Exception {
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

}
