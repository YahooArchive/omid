package com.yahoo.omid.tso;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import org.testng.annotations.Test;
import com.google.common.collect.Sets;

public class TestTimestampsOrdering extends TSOTestBase {

    @Test(timeOut=10000)
    public void testTimestampsOrdering() throws Exception {
        long timestamp;
        long tr1 = client.getNewStartTimestamp().get();
        timestamp = tr1;
        
        long tr2 = client.getNewStartTimestamp().get();
        assertEquals("Should grow monotonically", ++timestamp, tr2);

        long cr1 = client.commit(tr2, Sets.newHashSet(c1)).get();
        assertEquals("Should grow monotonically", ++timestamp, cr1);

        long cr2 = client.commit(tr1, Sets.newHashSet(c2)).get();
        assertEquals("Should grow monotonically", ++timestamp, cr2);

        long tr3 = client.getNewStartTimestamp().get();
        assertEquals("Should grow monotonically", ++timestamp, tr3);
   }
}
