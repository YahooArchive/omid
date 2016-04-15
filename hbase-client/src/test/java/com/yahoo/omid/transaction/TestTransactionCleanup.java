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
package com.yahoo.omid.transaction;

import com.google.common.util.concurrent.SettableFuture;
import com.yahoo.omid.tso.client.AbortException;
import com.yahoo.omid.tso.client.ForwardingTSOFuture;
import com.yahoo.omid.tso.client.TSOClient;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "sharedHBase")
public class TestTransactionCleanup extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestTransactionCleanup.class);

    private static final long START_TS = 1L;

    private byte[] row = Bytes.toBytes("row");
    private byte[] family = Bytes.toBytes(TEST_FAMILY);
    private byte[] qual = Bytes.toBytes("qual");
    private byte[] data = Bytes.toBytes("data");

    // NOTE: This test is maybe redundant with runTestCleanupAfterConflict()
    // and testCleanupWithDeleteRow() tests in TestTransactionCleanup class.
    // Code in TestTransactionCleanup is a little more difficult to follow,
    // lacks some assertions and includes some magic numbers, so we should
    // try to review and improve the tests in these two classes in a further
    // commit.
    @Test
    public void testTransactionIsCleanedUpAfterBeingAborted(ITestContext context) throws Exception {

        final int ROWS_MODIFIED = 1;

        // Prepare the mocking results
        SettableFuture<Long> startTSF = SettableFuture.create();
        startTSF.set(START_TS);
        ForwardingTSOFuture<Long> stFF = new ForwardingTSOFuture<>(startTSF);

        SettableFuture<Long> abortingF = SettableFuture.create();
        abortingF.setException(new AbortException());
        ForwardingTSOFuture<Long> abortingFF = new ForwardingTSOFuture<>(abortingF);

        // Mock the TSO Client setting the right method responses
        TSOClient mockedTSOClient = mock(TSOClient.class);

        doReturn(stFF)
                .when(mockedTSOClient).getNewStartTimestamp();

        doReturn(abortingFF)
                .when(mockedTSOClient).commit(eq(START_TS), anySetOf(HBaseCellId.class));

        try (TransactionManager tm = newTransactionManager(context, mockedTSOClient);
             TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            // Start a transaction and put some data in a column
            Transaction tx = tm.begin();

            Put put = new Put(row);
            put.add(family, qual, data);
            txTable.put(tx, put);

            // Abort transaction when committing, so the cleanup
            // process we want to test is triggered
            try {
                tm.commit(tx);
            } catch (RollbackException e) {
                // Expected
            }

            // So now we have to check that the Delete marker introduced by the
            // cleanup process is there
            Scan scan = new Scan(row);
            scan.setRaw(true); // Raw scan to obtain the deleted cells
            ResultScanner resultScanner = txTable.getHTable().getScanner(scan);
            int resultCount = 0;
            for (Result result : resultScanner) {
                assertEquals(2, result.size()); // Size == 2, including the put and delete from cleanup
                LOG.trace("Result {}", result);
                // The last element of the qualifier should have the Delete marker
                byte encodedType = result.getColumnLatestCell(family, qual).getTypeByte();
                assertEquals(KeyValue.Type.Delete,
                        KeyValue.Type.codeToType(encodedType));
                resultCount++;
            }
            assertEquals(ROWS_MODIFIED, resultCount);
        }
    }

}
