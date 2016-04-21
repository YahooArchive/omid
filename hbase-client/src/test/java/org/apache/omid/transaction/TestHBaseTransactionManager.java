/*
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
package org.apache.omid.transaction;

import org.apache.omid.tso.client.TSOClient;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

// TODO These tests should be adapted to a future test for AbstractTransactionManager as they should be DB independent
@Test(groups = "sharedHBase")
public class TestHBaseTransactionManager extends OmidTestBase {

    private static final int FAKE_EPOCH_INCREMENT = 100;

    private final byte[] row1 = Bytes.toBytes(TestHBaseTransactionManager.class.getCanonicalName());
    private final byte[] testFamily = Bytes.toBytes(TEST_FAMILY);
    private final byte[] qualifier = Bytes.toBytes("TEST_Q");
    private final byte[] data1 = Bytes.toBytes("test_data1");


    @Test(timeOut = 20_000)
    public void testTxManagerGetsTimestampsInTheRightEpoch(ITestContext context) throws Exception {

        TSOClient tsoClient = spy(getClient(context));

        long fakeEpoch = tsoClient.getNewStartTimestamp().get() + FAKE_EPOCH_INCREMENT;

        // Modify the epoch before testing the begin method
        doReturn(fakeEpoch).when(tsoClient).getEpoch();

        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context, tsoClient));

        // Create a transaction with the initial setup and check that the TX id matches the fake epoch created
        Transaction tx1 = tm.begin();
        assertEquals(tx1.getTransactionId(), fakeEpoch);
        verify(tsoClient, timeout(100).times(FAKE_EPOCH_INCREMENT)).getEpoch();

    }

    @Test(timeOut = 20_000)
    public void testReadOnlyTransactionsDoNotContactTSOServer(ITestContext context) throws Exception {

        final int EXPECTED_INVOCATIONS_FOR_COMMIT = 1; // Test specific checks

        TSOClient tsoClient = spy(getClient(context));
        TransactionManager tm = newTransactionManager(context, tsoClient);

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            // Add initial data in a transactional context
            Transaction tx1 = tm.begin();
            Put put = new Put(row1);
            put.add(testFamily, qualifier, data1);
            txTable.put(tx1, put);
            tm.commit(tx1);

            verify(tsoClient, times(EXPECTED_INVOCATIONS_FOR_COMMIT)).commit(anyLong(), anySetOf(HBaseCellId.class));

            // Create a read-only tx and verify that commit has not been invoked again in the TSOClient
            AbstractTransaction readOnlyTx = (AbstractTransaction) tm.begin();
            Get get = new Get(row1);
            Result r = txTable.get(readOnlyTx, get);
            assertTrue(Bytes.equals(r.getValue(testFamily, qualifier), data1), "Wrong value for RO-TX " + readOnlyTx);
            assertTrue(readOnlyTx.getWriteSet().isEmpty());
            tm.commit(readOnlyTx);

            verify(tsoClient, times(EXPECTED_INVOCATIONS_FOR_COMMIT)).commit(anyLong(), anySetOf(HBaseCellId.class));
            assertEquals(readOnlyTx.getStatus(), Transaction.Status.COMMITTED_RO);
        }

    }

}
