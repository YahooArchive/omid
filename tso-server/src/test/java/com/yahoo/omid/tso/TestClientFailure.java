package com.yahoo.omid.tso;

import static org.testng.AssertJUnit.assertTrue;
import org.testng.annotations.Test;
import org.testng.Assert;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.collect.Sets;
import com.yahoo.omid.tsoclient.CellId;
import com.yahoo.omid.tsoclient.TSOClient;
import com.yahoo.omid.tsoclient.TSOClientAccessor;

public class TestClientFailure extends TSOTestBase {

    public TestClientFailure() {
        super();
        clientConf.setProperty(TSOClient.REQUEST_MAX_RETRIES_CONFKEY, 0);
    }

    @Test(timeOut=10000)
    public void testCommitFailure() throws Exception {
        List<Long> startTimestamps = new ArrayList<Long>();
        for (int i = 0; i < 10; i++) {
            startTimestamps.add(client.getNewStartTimestamp().get());
        }

        pauseTSO();

        List<Future<Long>> futures = new ArrayList<Future<Long>>();
        for (long s : startTimestamps) {
            futures.add(client.commit(s, Sets.<CellId>newHashSet()));
        }
        TSOClientAccessor.closeChannel(client);

        for (Future<Long> f : futures) {
            try {
                f.get();
                Assert.fail("Shouldn't be able to complete");
            } catch (ExecutionException ee) {
                assertTrue("Should be a service unavailable exception",
                           ee.getCause() instanceof TSOClient.ServiceUnavailableException);
            }
        }
   }
}
