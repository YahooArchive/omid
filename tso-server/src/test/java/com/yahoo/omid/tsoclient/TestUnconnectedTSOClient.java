/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.tsoclient;

import com.yahoo.omid.tso.util.DummyCellIdImpl;
import com.yahoo.omid.tsoclient.TSOClient.DisconnectedState;
import com.yahoo.statemachine.StateMachine.FsmImpl;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Sets.newHashSet;
import static org.slf4j.LoggerFactory.getLogger;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Test the behavior of requests on a TSOClient component that is not connected to a TSO server.
 */
public class TestUnconnectedTSOClient {

    private static final Logger LOG = getLogger(TestUnconnectedTSOClient.class);

    private static final int TSO_RECONNECTION_DELAY_IN_SECS_FOR_TEST = 2;

    @Test(timeOut = 30_000) // 30 secs
    public void testRequestsDoneOnAnUnconnectedTSOClientAlwaysReturn() throws Exception {

        OmidClientConfiguration tsoClientConf = new OmidClientConfiguration();
        tsoClientConf.setConnectionString("localhost:12345");
        tsoClientConf.setReconnectionDelayInSecs(TSO_RECONNECTION_DELAY_IN_SECS_FOR_TEST);

        // Component under test
        TSOClient tsoClient = TSOClient.newInstance(tsoClientConf);

        // Internal accessor to fsm
        FsmImpl fsm = (FsmImpl) tsoClient.fsm;

        assertEquals(fsm.getState().getClass(), DisconnectedState.class);

        // Test requests to the 3 relevant methods in TSO client

        try {
            tsoClient.getNewStartTimestamp().get();
            fail();
        } catch (ExecutionException e) {
            LOG.info("Exception expected");
            assertEquals(e.getCause().getClass(), ConnectionException.class);
            TimeUnit.SECONDS.sleep(TSO_RECONNECTION_DELAY_IN_SECS_FOR_TEST * 2);
            assertEquals(fsm.getState().getClass(), DisconnectedState.class);
        }

        try {
            tsoClient.commit(1, newHashSet(new DummyCellIdImpl(0xdeadbeefL))).get();
            fail();
        } catch (ExecutionException e) {
            LOG.info("Exception expected");
            assertEquals(e.getCause().getClass(), ConnectionException.class);
            TimeUnit.SECONDS.sleep(TSO_RECONNECTION_DELAY_IN_SECS_FOR_TEST * 2);
            assertEquals(fsm.getState().getClass(), DisconnectedState.class);
        }

        tsoClient.close().get();
        LOG.info("No exception expected");
    }

}
