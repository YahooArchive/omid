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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.TestUtils;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.InMemoryCommitTable;
import org.apache.omid.transaction.Transaction.Status;
import org.apache.omid.tso.ProgrammableTSOServer;
import org.apache.omid.tso.client.TSOClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.io.IOException;

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

@Test(groups = "sharedHBase")
public class TestTxMgrFailover extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestTxMgrFailover.class);

    private static final int TSO_SERVER_PORT = 3333;
    private static final String TSO_SERVER_HOST = "localhost";

    private static final long TX1_ST = 1L;
    private static final long TX1_CT = 2L;

    private static final byte[] qualifier = Bytes.toBytes("test-qual");
    private static final byte[] row1 = Bytes.toBytes("row1");
    private static final byte[] data1 = Bytes.toBytes("testWrite-1");

    // Used in test assertions
    private InMemoryCommitTable commitTable;

    private CommitTable.Client commitTableClient;

    // Allows to prepare the required responses to client request operations
    private ProgrammableTSOServer tso;

    // The transaction manager under test
    private HBaseTransactionManager tm;

    @BeforeClass(alwaysRun = true)
    public void beforeClass() throws Exception {
        // ------------------------------------------------------------------------------------------------------------
        // ProgrammableTSOServer  setup
        // ------------------------------------------------------------------------------------------------------------
        tso = new ProgrammableTSOServer(TSO_SERVER_PORT);
        TestUtils.waitForSocketListening(TSO_SERVER_HOST, TSO_SERVER_PORT, 100);
    }

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void beforeMethod() throws IOException, InterruptedException {

        commitTable = new InMemoryCommitTable(); // Use an in-memory commit table to speed up tests
        commitTableClient = spy(commitTable.getClient());

        HBaseOmidClientConfiguration hbaseOmidClientConf = new HBaseOmidClientConfiguration();
        hbaseOmidClientConf.setConnectionString(TSO_SERVER_HOST + ":" + TSO_SERVER_PORT);
        hbaseOmidClientConf.setHBaseConfiguration(hbaseConf);
        TSOClient tsoClientForTM = spy(TSOClient.newInstance(hbaseOmidClientConf.getOmidClientConfiguration()));

        tm = spy(HBaseTransactionManager.builder(hbaseOmidClientConf)
                .tsoClient(tsoClientForTM)
                .commitTableClient(commitTableClient)
                .build());
    }

    @Test(timeOut = 30_000)
    public void testAbortResponseFromTSOThrowsRollbackExceptionInClient() throws Exception {
        // Program the TSO to return an ad-hoc Timestamp and an abort response for tx 1
        tso.queueResponse(new ProgrammableTSOServer.TimestampResponse(TX1_ST));
        tso.queueResponse(new ProgrammableTSOServer.AbortResponse(TX1_ST));

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            assertEquals(tx1.getStartTimestamp(), TX1_ST);
            Put put = new Put(row1);
            put.add(TEST_FAMILY.getBytes(), qualifier, data1);
            txTable.put(tx1, put);
            assertEquals(hBaseUtils.countRows(new HTable(hbaseConf, TEST_TABLE)), 1, "Rows should be 1!");
            checkOperationSuccessOnCell(KeyValue.Type.Put, data1, TEST_TABLE.getBytes(), row1, TEST_FAMILY.getBytes(),
                    qualifier);

            try {
                tm.commit(tx1);
                fail();
            } catch (RollbackException e) {
                // Expected!

            }

            // Check transaction status
            assertEquals(tx1.getStatus(), Status.ROLLEDBACK);
            assertEquals(tx1.getCommitTimestamp(), 0);
            // Check the cleanup process did its job and the committed data is NOT there
            checkOperationSuccessOnCell(KeyValue.Type.Delete, null, TEST_TABLE.getBytes(), row1, TEST_FAMILY.getBytes(),
                    qualifier);
        }

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    protected void checkOperationSuccessOnCell(KeyValue.Type targetOp,
                                               @Nullable byte[] expectedValue,
                                               byte[] tableName,
                                               byte[] row,
                                               byte[] fam,
                                               byte[] col) {

        try (HTable table = new HTable(hbaseConf, tableName)) {
            Get get = new Get(row).setMaxVersions(1);
            Result result = table.get(get);
            Cell latestCell = result.getColumnLatestCell(fam, col);

            switch (targetOp) {
                case Put:
                    assertEquals(latestCell.getTypeByte(), targetOp.getCode());
                    assertEquals(CellUtil.cloneValue(latestCell), expectedValue);
                    LOG.trace("Value for " + Bytes.toString(tableName) + ":"
                            + Bytes.toString(row) + ":" + Bytes.toString(fam) + ":"
                            + Bytes.toString(col) + "=>" + Bytes.toString(CellUtil.cloneValue(latestCell))
                            + " (" + Bytes.toString(expectedValue) + " expected)");
                    break;
                case Delete:
                    LOG.trace("Value for " + Bytes.toString(tableName) + ":"
                            + Bytes.toString(row) + ":" + Bytes.toString(fam)
                            + Bytes.toString(col) + " deleted");
                    assertNull(latestCell);
                    break;
                default:
                    fail();
            }
        } catch (IOException e) {
            LOG.error("Error reading row " + Bytes.toString(tableName) + ":"
                    + Bytes.toString(row) + ":" + Bytes.toString(fam)
                    + Bytes.toString(col), e);
            fail();
        }
    }

}
