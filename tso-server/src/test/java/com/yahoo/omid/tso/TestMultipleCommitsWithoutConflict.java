package com.yahoo.omid.tso;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import org.testng.annotations.Test;
import com.google.common.collect.Sets;


public class TestMultipleCommitsWithoutConflict extends TSOTestBase {

    @Test(timeOut=10000)
    public void testMultipleCommitsWithoutConflict() throws Exception {
        long tr1 = client.getNewStartTimestamp().get();
        long cr1 = client.commit(tr1, Sets.newHashSet(c1)).get();
        assertTrue("commit ts must be greater than start ts", cr1 > tr1);

        long tr2 = client.getNewStartTimestamp().get();
        assertTrue("ts should grow monotonically", tr2 > cr1);

        long cr2 = client.commit(tr2, Sets.newHashSet(c1, c2)).get();
        assertTrue("commit ts must be greater than start ts", cr2 > tr2);

        long tr3 = client.getNewStartTimestamp().get();
        long cr3 = client.commit(tr3, Sets.newHashSet(c2)).get();
        assertTrue("commit ts must be greater than start ts", cr3 > tr3);
    }
}
