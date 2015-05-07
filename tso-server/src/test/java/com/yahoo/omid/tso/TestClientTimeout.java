package com.yahoo.omid.tso;

import org.testng.annotations.Test;
import java.util.concurrent.Future;

import static com.yahoo.omid.tsoclient.TSOClient.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestClientTimeout extends TSOTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TestClientTimeout.class);

    /**
     * Test to repro issue described in http://bug.corp.yahoo.com/show_bug.cgi?id=7120763
     * We send a lot of timestamp requests, and wait for them to complete.
     * Ensure that the next request doesn't get hit by the timeouts of the previous
     * requests. (i.e. make sure we cancel timeouts)
     */
    @Test(timeOut=100000)
    public void testTimeouts() throws Exception {
        int requestTimeoutMs = clientConf.getInt(REQUEST_TIMEOUT_IN_MS_CONFKEY, DEFAULT_REQUEST_TIMEOUT_MS);
        int requestMaxRetries = clientConf.getInt(REQUEST_MAX_RETRIES_CONFKEY, DEFAULT_TSO_MAX_REQUEST_RETRIES);
        Future<Long> f = null;
        for (int i = 0; i < requestMaxRetries*10; i++) {
            f = client.getNewStartTimestamp();
        }
        if (f != null) {
            f.get();
        }
        pauseTSO();
        Thread.sleep((int)(requestTimeoutMs*0.75));
        f = client.getNewStartTimestamp();
        Thread.sleep((int)(requestTimeoutMs*0.9));
        LOG.info("Resuming");
        resumeTSO();
        f.get();
    }

}
