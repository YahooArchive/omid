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

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;


import com.yahoo.omid.client.TSOClient;
import com.yahoo.omid.client.TSOFuture;
import com.yahoo.omid.client.TSOClient.AbortException;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CleanedTransactionReport;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.FullAbortRequest;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.TimestampResponse;

import java.util.concurrent.ExecutionException;

public class TestBasicTransaction extends TSOTestBase {

    @Test
    public void testConflicts() throws Exception {
        long tr1 = client.createTransaction().get();
        long tr2 = client.createTransaction().get();

        long cr1 = client.commit(tr1, new RowKey[] { r1 }).get();

        try {
            long cr2 = client.commit(tr2, new RowKey[] { r1, r2 }).get();
            fail("Shouldn't have committed");
        } catch (ExecutionException ee) {
            assertEquals("Should have aborted", ee.getCause().getClass(), AbortException.class);
        }

        // TODO fix this. Commiting an already committed value (either failed or commited) should fail
        // FIXME to do this.
        //      clientHandler.sendMessage(new CommitRequest(tr1.timestamp, new RowKey[] { r2 }));
        //      messageReceived = clientHandler.receiveMessage();
        //      assertThat(messageReceived, is(CommitResponse.class));
        //      CommitResponse cr3 = (CommitResponse) messageReceived;
        //      assertFalse(cr3.committed);
    }

}
