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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.ExecutionException;

import org.testng.annotations.Test;

import com.yahoo.omid.proto.TSOProto;
import com.yahoo.omid.tsoclient.TSOClient;

public class TestHandshake extends TSOTestBase {
    /**
     * Test that if a client tries to make a request
     * without handshaking, it will be disconnected.
     */
    @Test(timeOut=10000)
    public void testOldClientCurrentServer() throws Exception {
        TSOClientRaw raw = new TSOClientRaw(clientConf.getString("tso.host"),
                                            clientConf.getInt("tso.port"));

        TSOProto.Request request = TSOProto.Request.newBuilder()
            .setTimestampRequest(TSOProto.TimestampRequest.newBuilder().build())
            .build();
        raw.write(request);
        try {
            raw.getResponse().get();
            fail("Channel should close");
        } catch (ExecutionException ee) {
            assertEquals("Should be channel closed exception",
                    TSOClient.ConnectionException.class, ee.getCause().getClass());
        }
        raw.close();
   }
}
