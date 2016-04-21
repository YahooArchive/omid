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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(groups = "sharedHBase")
public class TestAsynchronousPostCommitter extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestAsynchronousPostCommitter.class);

    private static final byte[] family = Bytes.toBytes(TEST_FAMILY);
    private static final byte[] nonExistentFamily = Bytes.toBytes("non-existent");
    private static final byte[] qualifier = Bytes.toBytes("test-qual");

    byte[] row1 = Bytes.toBytes("test-is-committed1");
    byte[] row2 = Bytes.toBytes("test-is-committed2");

    @Test(timeOut = 30_000)
    public void testPostCommitActionsAreCalledAsynchronously(ITestContext context) throws Exception {

        CommitTable.Client commitTableClient = getCommitTable(context).getClient();

        PostCommitActions syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(), commitTableClient));
        ListeningExecutorService postCommitExecutor =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder().setNameFormat("postCommit-%d").build()));
        PostCommitActions asyncPostCommitter = new HBaseAsyncPostCommitter(syncPostCommitter, postCommitExecutor);

        TransactionManager tm = newTransactionManager(context, asyncPostCommitter);

        final CountDownLatch beforeUpdatingShadowCellsLatch = new CountDownLatch(1);
        final CountDownLatch afterUpdatingShadowCellsLatch = new CountDownLatch(1);
        final CountDownLatch beforeRemovingCTEntryLatch = new CountDownLatch(1);
        final CountDownLatch afterRemovingCTEntryLatch = new CountDownLatch(1);

        doAnswer(new Answer<ListenableFuture<Void>>() {
            public ListenableFuture<Void> answer(InvocationOnMock invocation) {
                try {
                    beforeUpdatingShadowCellsLatch.await();
                    invocation.callRealMethod();
                    afterUpdatingShadowCellsLatch.countDown();
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
                return SettableFuture.create();
            }
        }).when(syncPostCommitter).updateShadowCells(any(AbstractTransaction.class));

        doAnswer(new Answer<ListenableFuture<Void>>() {
            public ListenableFuture<Void> answer(InvocationOnMock invocation) {
                try {
                    beforeRemovingCTEntryLatch.await();
                    LOG.info("We are here");
                    invocation.callRealMethod();
                    afterRemovingCTEntryLatch.countDown();
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
                return SettableFuture.create();
            }
        }).when(syncPostCommitter).removeCommitTableEntry(any(AbstractTransaction.class));

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            // Execute tx with async post commit actions
            Transaction tx1 = tm.begin();

            Put put1 = new Put(row1);
            put1.add(family, qualifier, Bytes.toBytes("hey!"));
            txTable.put(tx1, put1);
            Put put2 = new Put(row2);
            put2.add(family, qualifier, Bytes.toBytes("hou!"));
            txTable.put(tx1, put2);

            tm.commit(tx1);

            long tx1Id = tx1.getTransactionId();

            // As we have paused the update of shadow cells, the shadow cells shouldn't be there yet
            assertFalse(CellUtils.hasShadowCell(row1, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            assertFalse(CellUtils.hasShadowCell(row2, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));

            // Commit Table should contain an entry for the transaction
            Optional<CommitTable.CommitTimestamp> commitTimestamp = commitTableClient.getCommitTimestamp(tx1Id).get();
            assertTrue(commitTimestamp.isPresent());
            assertTrue(commitTimestamp.get().isValid());
            assertEquals(commitTimestamp.get().getValue(), ((AbstractTransaction) tx1).getCommitTimestamp());

            // Read from row1 and row2 in a different Tx and check that result is the data written by tx1 despite the
            // post commit actions have not been executed yet (the shadow cells healing process should make its work)
            Transaction tx2 = tm.begin();
            Get get1 = new Get(row1);
            Result result = txTable.get(tx2, get1);
            byte[] value =  result.getValue(family, qualifier);
            assertNotNull(value);
            assertEquals("hey!", Bytes.toString(value));

            Get get2 = new Get(row2);
            result = txTable.get(tx2, get2);
            value = result.getValue(family, qualifier);
            assertNotNull(value);
            assertEquals("hou!", Bytes.toString(value));

            // Then, we continue with the update of shadow cells and we wait till completed
            beforeUpdatingShadowCellsLatch.countDown();
            afterUpdatingShadowCellsLatch.await();

            // Now we can check that the shadow cells are there...
            verify(syncPostCommitter, times(1)).updateShadowCells(any(AbstractTransaction.class));
            assertTrue(CellUtils.hasShadowCell(row1, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            assertTrue(CellUtils.hasShadowCell(row2, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            // ...and the transaction entry is still in the Commit Table
            commitTimestamp = commitTableClient.getCommitTimestamp(tx1Id).get();
            assertTrue(commitTimestamp.isPresent());
            assertTrue(commitTimestamp.get().isValid());
            assertEquals(commitTimestamp.get().getValue(), ((AbstractTransaction) tx1).getCommitTimestamp());

            // Finally, we continue till the Commit Table cleaning process is done...
            beforeRemovingCTEntryLatch.countDown();
            afterRemovingCTEntryLatch.await();

            // ...so now, the Commit Table should NOT contain the entry for the transaction anymore
            verify(syncPostCommitter, times(1)).removeCommitTableEntry(any(AbstractTransaction.class));
            commitTimestamp = commitTableClient.getCommitTimestamp(tx1Id).get();
            assertFalse(commitTimestamp.isPresent());

            // Final checks
            verify(syncPostCommitter, times(1)).updateShadowCells(any(AbstractTransaction.class));
            verify(syncPostCommitter, times(1)).removeCommitTableEntry(any(AbstractTransaction.class));

        }

    }

    @Test(timeOut = 30_000)
    public void testNoAsyncPostActionsAreCalled(ITestContext context) throws Exception {

        CommitTable.Client commitTableClient = getCommitTable(context).getClient();

        PostCommitActions syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(), commitTableClient));
        ListeningExecutorService postCommitExecutor =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder().setNameFormat("postCommit-%d").build()));
        PostCommitActions asyncPostCommitter = new HBaseAsyncPostCommitter(syncPostCommitter, postCommitExecutor);

        TransactionManager tm = newTransactionManager(context, asyncPostCommitter);

        final CountDownLatch updateShadowCellsCalledLatch = new CountDownLatch(1);
        final CountDownLatch removeCommitTableEntryCalledLatch = new CountDownLatch(1);

        // Simulate shadow cells are not updated and commit table is not clean
        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                // Do not invoke real method simulating a fail of the shadow cells update
                updateShadowCellsCalledLatch.countDown();
                return null;
            }
        }).when(syncPostCommitter).updateShadowCells(any(AbstractTransaction.class));

        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                // Do not invoke real method simulating a fail of the async clean of commit table entry
                removeCommitTableEntryCalledLatch.countDown();
                return null;
            }
        }).when(syncPostCommitter).removeCommitTableEntry(any(AbstractTransaction.class));


        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            // Execute tx with async post commit actions
            Transaction tx1 = tm.begin();

            Put put1 = new Put(row1);
            put1.add(family, qualifier, Bytes.toBytes("hey!"));
            txTable.put(tx1, put1);
            Put put2 = new Put(row2);
            put2.add(family, qualifier, Bytes.toBytes("hou!"));
            txTable.put(tx1, put2);

            tm.commit(tx1);

            long tx1Id = tx1.getTransactionId();

            // The shadow cells shouldn't be there...
            assertFalse(CellUtils.hasShadowCell(row1, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            assertFalse(CellUtils.hasShadowCell(row2, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            // ... and the should NOT have been cleaned
            Optional<CommitTable.CommitTimestamp> commitTimestamp = commitTableClient.getCommitTimestamp(tx1Id).get();
            assertTrue(commitTimestamp.isPresent());
            assertTrue(commitTimestamp.get().isValid());

            updateShadowCellsCalledLatch.await();

            // Not even after waiting for the method call on the shadow cells update...
            assertFalse(CellUtils.hasShadowCell(row1, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            assertFalse(CellUtils.hasShadowCell(row2, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));

            removeCommitTableEntryCalledLatch.await();
            // ... and after waiting for the method call for cleaning the commit table entry
            commitTimestamp = commitTableClient.getCommitTimestamp(tx1Id).get();
            assertTrue(commitTimestamp.isPresent());
            assertTrue(commitTimestamp.get().isValid());

            // Final checks
            verify(syncPostCommitter, times(1)).updateShadowCells(any(AbstractTransaction.class));
            verify(syncPostCommitter, times(1)).removeCommitTableEntry(any(AbstractTransaction.class));

        }

    }

    @Test(timeOut = 30_000)
    public void testOnlyShadowCellsUpdateIsExecuted(ITestContext context) throws Exception {

        CommitTable.Client commitTableClient = getCommitTable(context).getClient();

        PostCommitActions syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(), commitTableClient));
        ListeningExecutorService postCommitExecutor =
                MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(
                        new ThreadFactoryBuilder().setNameFormat("postCommit-%d").build()));
        PostCommitActions asyncPostCommitter = new HBaseAsyncPostCommitter(syncPostCommitter, postCommitExecutor);

        TransactionManager tm = newTransactionManager(context, asyncPostCommitter);

        final CountDownLatch removeCommitTableEntryCalledLatch = new CountDownLatch(1);

        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                // Do not invoke real method simulating a fail of the async clean of commit table entry
                removeCommitTableEntryCalledLatch.countDown();
                return null;
            }
        }).when(syncPostCommitter).removeCommitTableEntry(any(AbstractTransaction.class));


        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            // Execute tx with async post commit actions
            Transaction tx1 = tm.begin();

            Put put1 = new Put(row1);
            put1.add(family, qualifier, Bytes.toBytes("hey!"));
            txTable.put(tx1, put1);
            Put put2 = new Put(row2);
            put2.add(family, qualifier, Bytes.toBytes("hou!"));
            txTable.put(tx1, put2);

            tm.commit(tx1);

            long tx1Id = tx1.getTransactionId();

            // We continue when the unsuccessful call of the method for cleaning commit table has been invoked
            removeCommitTableEntryCalledLatch.await();

            // We check that the shadow cells are there (because the update of the shadow cells should precede
            // the cleaning of the commit table entry) ...
            assertTrue(CellUtils.hasShadowCell(row1, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            assertTrue(CellUtils.hasShadowCell(row2, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));

            // ... and the commit table entry has NOT been cleaned
            Optional<CommitTable.CommitTimestamp> commitTimestamp = commitTableClient.getCommitTimestamp(tx1Id).get();
            assertTrue(commitTimestamp.isPresent());
            assertTrue(commitTimestamp.get().isValid());

            // Final checks
            verify(syncPostCommitter, times(1)).updateShadowCells(any(AbstractTransaction.class));
            verify(syncPostCommitter, times(1)).removeCommitTableEntry(any(AbstractTransaction.class));

        }

    }

}
