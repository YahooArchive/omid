package com.yahoo.omid.tso;

import static org.testng.AssertJUnit.assertTrue;
import org.testng.annotations.Test;


public class TestTimestamps extends TSOTestBase {

    @Test(timeOut=10000)
    public void testGetTimestamp() throws Exception {
        long tr1 = client.getNewStartTimestamp().get();
        long tr2 = client.getNewStartTimestamp().get();;
        assertTrue("timestamps should grow", tr2 > tr1);
    }
   
}
