/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

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
