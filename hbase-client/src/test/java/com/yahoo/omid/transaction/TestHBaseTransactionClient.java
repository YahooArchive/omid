package com.yahoo.omid.transaction;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.CommitTimestamp;

import static com.yahoo.omid.committable.CommitTable.CommitTimestamp.Location.NOT_PRESENT;
import static com.yahoo.omid.committable.CommitTable.CommitTimestamp.Location.COMMIT_TABLE;
import static com.yahoo.omid.committable.CommitTable.CommitTimestamp.Location.SHADOW_CELL;
import static com.yahoo.omid.committable.CommitTable.CommitTimestamp.Location.CACHE;

import com.yahoo.omid.transaction.HBaseTransactionManager.CommitTimestampLocatorImpl;

@Test(groups = "sharedHBase")
public class TestHBaseTransactionClient extends OmidTestBase {

    private static final byte[] row1 = Bytes.toBytes("test-is-committed1");
    private static final byte[] row2 = Bytes.toBytes("test-is-committed2");
    private static final byte[] family = Bytes.toBytes(TEST_FAMILY);
    private static final byte[] qualifier = Bytes.toBytes("testdata");
    private static final byte[] data1 = Bytes.toBytes("testWrite-1");

    @Test
    public void testIsCommitted(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        Put put = new Put(row1);
        put.add(family, qualifier, data1);
        table.put(t1, put);
        tm.commit(t1);

        HBaseTransaction t2 = (HBaseTransaction) tm.begin();
        put = new Put(row2);
        put.add(family, qualifier, data1);
        table.put(t2, put);
        table.getHTable().flushCommits();

        HBaseTransaction t3 = (HBaseTransaction) tm.begin();
        put = new Put(row2);
        put.add(family, qualifier, data1);
        table.put(t3, put);
        tm.commit(t3);

        HTable htable = new HTable(hbaseConf, TEST_TABLE);
        HBaseCellId hBaseCellId1 = new HBaseCellId(htable, row1, family, qualifier, t1.getStartTimestamp());
        HBaseCellId hBaseCellId2 = new HBaseCellId(htable, row2, family, qualifier, t2.getStartTimestamp());
        HBaseCellId hBaseCellId3 = new HBaseCellId(htable, row2, family, qualifier, t3.getStartTimestamp());

        HBaseTransactionClient hbaseTm = (HBaseTransactionClient) newTransactionManager(context);
        assertTrue("row1 should be committed", hbaseTm.isCommitted(hBaseCellId1));
        assertFalse("row2 should not be committed for kv2", hbaseTm.isCommitted(hBaseCellId2));
        assertTrue("row2 should be committed for kv3", hbaseTm.isCommitted(hBaseCellId3));
    }

    @Test
    public void testCrashAfterCommit(ITestContext context) throws Exception {
        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context));
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(tm).updateShadowCells(any(HBaseTransaction.class));

        TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        // Test shadow cell are created properly
        Put put = new Put(row1);
        put.add(family, qualifier, data1);
        table.put(t1, put);
        try {
            tm.commit(t1);
        } catch (Exception e) { // (*) crash
            // Do nothing
        }

        assertTrue("Cell should be there",
                CellUtils.hasCell(row1,
                                   family,
                                   qualifier,
                                   t1.getStartTimestamp(),
                                   new TTableCellGetterAdapter(table)));
        assertFalse("Shadow cell should not be there",
                CellUtils.hasShadowCell(row1,
                                         family,
                                         qualifier,
                                         t1.getStartTimestamp(),
                                         new TTableCellGetterAdapter(table)));

        HTable htable = new HTable(hbaseConf, TEST_TABLE);
        HBaseCellId hBaseCellId = new HBaseCellId(htable, row1, family, qualifier, t1.getStartTimestamp());

        HBaseTransactionClient hbaseTm = (HBaseTransactionClient) newTransactionManager(context);
        assertTrue("row1 should be committed", hbaseTm.isCommitted(hBaseCellId));
    }

    @Test(timeOut = 30000)
    public void testReadCommitTimestampFromCommitTable(ITestContext context) throws Exception {

        final long NON_EXISTING_CELL_TS = 1000L;

        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context));
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(tm).updateShadowCells(any(HBaseTransaction.class));

        // Test that a non-existing cell timestamp returns an empty result
        Optional<CommitTimestamp> optionalCT = tm.commitTableClient.getCommitTimestamp(NON_EXISTING_CELL_TS).get();
        assertFalse(optionalCT.isPresent());

        try (TTable table = new TTable(hbaseConf, TEST_TABLE)) {
            // Test that we get an invalidation mark for an invalidated transaction

            // Start a transaction and invalidate it before commiting it
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.add(family, qualifier, data1);
            table.put(tx1, put);

            assertTrue(tm.commitTableClient.tryInvalidateTransaction(tx1.getStartTimestamp()).get());
            optionalCT = tm.commitTableClient.getCommitTimestamp(tx1.getStartTimestamp()).get();
            assertTrue(optionalCT.isPresent());
            CommitTimestamp ct = optionalCT.get();
            assertFalse(ct.isValid());
            assertEquals(CommitTable.INVALID_TRANSACTION_MARKER, ct.getValue());
            assertTrue(ct.getLocation().compareTo(COMMIT_TABLE) == 0);

            // Finally test that we get the right commit timestamp for a committed tx
            // that couldn't get
            HBaseTransaction tx2 = (HBaseTransaction) tm.begin();
            Put otherPut = new Put(row1);
            otherPut.add(family, qualifier, data1);
            table.put(tx2, otherPut);
            try {
                tm.commit(tx2);
            } catch (Exception e) { // (*) crash
                // Do nothing
            }

            optionalCT = tm.commitTableClient.getCommitTimestamp(tx2.getStartTimestamp()).get();
            assertTrue(optionalCT.isPresent());
            ct = optionalCT.get();
            assertTrue(ct.isValid());
            assertEquals(tx2.getCommitTimestamp(), ct.getValue());
            assertTrue(ct.getLocation().compareTo(COMMIT_TABLE) == 0);
        }
    }

    @Test(timeOut = 30000)
    public void testReadCommitTimestampFromShadowCell(ITestContext context) throws Exception {

        final long NON_EXISTING_CELL_TS = 1L;

        HBaseTransactionManager tm = (HBaseTransactionManager) newTransactionManager(context);

        try (TTable table = new TTable(hbaseConf, TEST_TABLE)) {

            // Test first we can not found a non-existent cell ts
            HBaseCellId hBaseCellId = new HBaseCellId(table.getHTable(), row1, family, qualifier, NON_EXISTING_CELL_TS);
            // Set an empty cache to allow to bypass the checking
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long> newHashMap());
            Optional<CommitTimestamp> optionalCT = tm
                    .readCommitTimestampFromShadowCell(NON_EXISTING_CELL_TS, ctLocator);
            assertFalse(optionalCT.isPresent());

            // Then test that for a transaction committed, we get the right CT
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.add(family, qualifier, data1);
            table.put(tx1, put);
            tm.commit(tx1);
            // Upon commit, the commit data should be in the shadow cells, so test it
            optionalCT = tm.readCommitTimestampFromShadowCell(tx1.getStartTimestamp(), ctLocator);
            assertTrue(optionalCT.isPresent());
            CommitTimestamp ct = optionalCT.get();
            assertTrue(ct.isValid());
            assertEquals(tx1.getCommitTimestamp(), ct.getValue());
            assertTrue(ct.getLocation().compareTo(SHADOW_CELL) == 0);

        }

    }

    // Tests step 1 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test(timeOut = 30000)
    public void testCellCommitTimestampIsLocatedInCache(ITestContext context) throws Exception {

        final long CELL_ST = 1L;
        final long CELL_CT = 2L;

        HBaseTransactionManager tm = (HBaseTransactionManager) newTransactionManager(context);

        // Pre-load the element to look for in the cache
        HTable table = new HTable(hbaseConf, TEST_TABLE);
        HBaseCellId hBaseCellId = new HBaseCellId(table, row1, family, qualifier, CELL_ST);
        Map<Long, Long> fakeCache = Maps.<Long, Long> newHashMap();
        fakeCache.put(CELL_ST, CELL_CT);

        // Then test that locator finds it in the cache
        CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId, fakeCache);
        CommitTimestamp ct = tm.locateCellCommitTimestamp(CELL_ST, tm.tsoClient.getEpoch(), ctLocator);
        assertTrue(ct.isValid());
        assertEquals(ct.getValue(), CELL_CT);
        assertTrue(ct.getLocation().compareTo(CACHE) == 0);

    }

    // Tests step 2 in AbstractTransactionManager.locateCellCommitTimestamp()
    // Note: This test is very similar to testCrashAfterCommit() above so
    // maybe we should merge them in this test, adding the missing assertions
    @Test(timeOut = 30000)
    public void testCellCommitTimestampIsLocatedInCommitTable(ITestContext context) throws Exception {

        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context));
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(tm).updateShadowCells(any(HBaseTransaction.class));

        try (TTable table = new TTable(hbaseConf, TEST_TABLE)) {
            // Commit a transaction that is broken on commit to avoid
            // write to the shadow cells and avoid cleaning the commit table
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.add(family, qualifier, data1);
            table.put(tx1, put);
            try {
                tm.commit(tx1);
            } catch (Exception e) { // (*) crash
                // Do nothing
            }

            // Test the locator finds the appropriate data in the commit table
            HBaseCellId hBaseCellId = new HBaseCellId(table.getHTable(), row1, family, qualifier,
                    tx1.getStartTimestamp());
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long> newHashMap());
            CommitTimestamp ct = tm.locateCellCommitTimestamp(tx1.getStartTimestamp(), tm.tsoClient.getEpoch(),
                    ctLocator);
            assertTrue(ct.isValid());
            long expectedCommitTS = tx1.getStartTimestamp() + 1;
            assertEquals(expectedCommitTS, ct.getValue());
            assertTrue(ct.getLocation().compareTo(COMMIT_TABLE) == 0);
        }

    }

    // Tests step 3 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test(timeOut = 30000)
    public void testCellCommitTimestampIsLocatedInShadowCells(ITestContext context) throws Exception {

        HBaseTransactionManager tm = (HBaseTransactionManager) newTransactionManager(context);

        try (TTable table = new TTable(hbaseConf, TEST_TABLE)) {
            // Commit a transaction to add ST/CT in commit table
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.add(family, qualifier, data1);
            table.put(tx1, put);
            tm.commit(tx1);
            // Upon commit, the commit data should be in the shadow cells

            // Test the locator finds the appropriate data in the shadow cells
            HBaseCellId hBaseCellId = new HBaseCellId(table.getHTable(), row1, family, qualifier,
                    tx1.getStartTimestamp());
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long> newHashMap());
            CommitTimestamp ct = tm.locateCellCommitTimestamp(tx1.getStartTimestamp(), tm.tsoClient.getEpoch(),
                    ctLocator);
            assertTrue(ct.isValid());
            assertEquals(tx1.getCommitTimestamp(), ct.getValue());
            assertTrue(ct.getLocation().compareTo(SHADOW_CELL) == 0);
        }

    }

    // Tests step 4 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test(timeOut = 30000)
    public void testCellFromTransactionInPreviousEpochGetsInvalidComitTimestamp(ITestContext context) throws Exception {

        final long CURRENT_EPOCH_FAKE = 1000L;

        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient().get());
        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context, commitTableClient));
        // The following lines allow to reach step 4)
        // in AbstractTransactionManager.locateCellCommitTimestamp()
        SettableFuture<Optional<CommitTimestamp>> f = SettableFuture.<Optional<CommitTimestamp>> create();
        f.set(Optional.<CommitTimestamp> absent());
        doReturn(f).when(commitTableClient).getCommitTimestamp(any(Long.class));
        doReturn(Optional.<CommitTimestamp> absent()).when(tm).readCommitTimestampFromShadowCell(any(Long.class),
                any(CommitTimestampLocator.class));

        try (TTable table = new TTable(hbaseConf, TEST_TABLE)) {

            // Commit a transaction to add ST/CT in commit table
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.add(family, qualifier, data1);
            table.put(tx1, put);
            tm.commit(tx1);
            // Upon commit, the commit data should be in the shadow cells

            // Test a transaction in the previous epoch gets an InvalidCommitTimestamp class
            HBaseCellId hBaseCellId = new HBaseCellId(table.getHTable(), row1, family, qualifier,
                    tx1.getStartTimestamp());
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long> newHashMap());
            // Fake the current epoch to simulate a newer TSO
            CommitTimestamp ct = tm.locateCellCommitTimestamp(tx1.getStartTimestamp(), CURRENT_EPOCH_FAKE, ctLocator);
            assertFalse(ct.isValid());
            assertEquals(CommitTable.INVALID_TRANSACTION_MARKER, ct.getValue());
            assertTrue(ct.getLocation().compareTo(COMMIT_TABLE) == 0);
        }
    }

    // Tests step 5 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test(timeOut = 30000)
    public void testCellCommitTimestampIsLocatedInCommitTableAfterNotBeingInvalidated(ITestContext context) throws Exception {

        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient().get());
        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context, commitTableClient));
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(tm).updateShadowCells(any(HBaseTransaction.class));
        // The next two lines avoid steps 2) and 3) and go directly to step 5)
        // in AbstractTransactionManager.locateCellCommitTimestamp()
        SettableFuture<Optional<CommitTimestamp>> f = SettableFuture.<Optional<CommitTimestamp>> create();
        f.set(Optional.<CommitTimestamp> absent());
        doReturn(f).doCallRealMethod().when(commitTableClient).getCommitTimestamp(any(Long.class));
        doReturn(Optional.<CommitTimestamp> absent()).when(tm).readCommitTimestampFromShadowCell(any(Long.class),
                any(CommitTimestampLocator.class));

        try (TTable table = new TTable(hbaseConf, TEST_TABLE)) {

            // Commit a transaction that is broken on commit to avoid
            // write to the shadow cells and avoid cleaning the commit table
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.add(family, qualifier, data1);
            table.put(tx1, put);
            try {
                tm.commit(tx1);
            } catch (Exception e) { // (*) crash
                // Do nothing
            }

            // Test the locator finds the appropriate data in the commit table
            HBaseCellId hBaseCellId = new HBaseCellId(table.getHTable(), row1, family, qualifier,
                    tx1.getStartTimestamp());
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long> newHashMap());
            CommitTimestamp ct = tm.locateCellCommitTimestamp(tx1.getStartTimestamp(), tm.tsoClient.getEpoch(),
                    ctLocator);
            assertTrue(ct.isValid());
            assertEquals(tx1.getCommitTimestamp(), ct.getValue());
            assertTrue(ct.getLocation().compareTo(COMMIT_TABLE) == 0);
        }

    }

    // Tests step 6 in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test(timeOut = 30000)
    public void testCellCommitTimestampIsLocatedInShadowCellsAfterNotBeingInvalidated(ITestContext context) throws Exception {

        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient().get());
        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context, commitTableClient));
        // The next two lines avoid steps 2), 3) and 5) and go directly to step 6)
        // in AbstractTransactionManager.locateCellCommitTimestamp()
        SettableFuture<Optional<CommitTimestamp>> f = SettableFuture.<Optional<CommitTimestamp>> create();
        f.set(Optional.<CommitTimestamp> absent());
        doReturn(f).when(commitTableClient).getCommitTimestamp(any(Long.class));
        doReturn(Optional.<CommitTimestamp> absent()).doCallRealMethod()
                .when(tm).readCommitTimestampFromShadowCell(any(Long.class), any(CommitTimestampLocator.class));

        try (TTable table = new TTable(hbaseConf, TEST_TABLE)) {

            // Commit a transaction to add ST/CT in commit table
            HBaseTransaction tx1 = (HBaseTransaction) tm.begin();
            Put put = new Put(row1);
            put.add(family, qualifier, data1);
            table.put(tx1, put);
            tm.commit(tx1);
            // Upon commit, the commit data should be in the shadow cells

            // Test the locator finds the appropriate data in the shadow cells
            HBaseCellId hBaseCellId = new HBaseCellId(table.getHTable(), row1, family, qualifier,
                    tx1.getStartTimestamp());
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long> newHashMap());
            CommitTimestamp ct = tm.locateCellCommitTimestamp(tx1.getStartTimestamp(), tm.tsoClient.getEpoch(),
                    ctLocator);
            assertTrue(ct.isValid());
            assertEquals(tx1.getCommitTimestamp(), ct.getValue());
            assertTrue(ct.getLocation().compareTo(SHADOW_CELL) == 0);
        }

    }

    // Tests last step in AbstractTransactionManager.locateCellCommitTimestamp()
    @Test(timeOut = 30000)
    public void testCTLocatorReturnsAValidCTWhenNotPresent(ITestContext context) throws Exception {

        final long CELL_TS = 1L;

        CommitTable.Client commitTableClient = spy(getCommitTable(context).getClient().get());
        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager(context, commitTableClient));
        // The following lines allow to reach the last return statement
        SettableFuture<Optional<CommitTimestamp>> f = SettableFuture.<Optional<CommitTimestamp>> create();
        f.set(Optional.<CommitTimestamp> absent());
        doReturn(f).when(commitTableClient).getCommitTimestamp(any(Long.class));
        doReturn(Optional.<CommitTimestamp> absent()).when(tm).readCommitTimestampFromShadowCell(any(Long.class),
                any(CommitTimestampLocator.class));

        try (TTable table = new TTable(hbaseConf, TEST_TABLE)) {
            HBaseCellId hBaseCellId = new HBaseCellId(table.getHTable(), row1, family, qualifier, CELL_TS);
            CommitTimestampLocator ctLocator = new CommitTimestampLocatorImpl(hBaseCellId,
                    Maps.<Long, Long> newHashMap());
            CommitTimestamp ct = tm.locateCellCommitTimestamp(CELL_TS, tm.tsoClient.getEpoch(), ctLocator);
            assertTrue(ct.isValid());
            assertEquals(-1L, ct.getValue());
            assertTrue(ct.getLocation().compareTo(NOT_PRESENT) == 0);
        }
    }

}
