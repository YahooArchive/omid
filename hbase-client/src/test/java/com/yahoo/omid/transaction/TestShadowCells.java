package com.yahoo.omid.transaction;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.tsoclient.TSOClient;

public class TestShadowCells extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestShadowCells.class);

    static final byte[] row = Bytes.toBytes("test-sc");
    static final byte[] family = Bytes.toBytes(TEST_FAMILY);
    static final byte[] qualifier = Bytes.toBytes("testdata");
    static final byte[] data1 = Bytes.toBytes("testWrite-1");
    static final byte[] data2 = Bytes.toBytes("testWrite-2");

    @Test
    public void testShadowCellsBasics() throws Exception {

        TransactionManager tm = newTransactionManager();

        TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        // Test shadow cell are created properly
        Put put = new Put(row);
        put.add(family, qualifier, data1);
        table.put(t1, put);
        assertTrue("Cell should be there",
                hasCell(table, row, family, qualifier, t1.getStartTimestamp()));
        assertFalse("Shadow cell shouldn't be there",
                hasShadowCell(table, row, family, qualifier, t1.getStartTimestamp()));
        tm.commit(t1);
        assertTrue("Cell should be there",
                hasCell(table, row, family, qualifier, t1.getStartTimestamp()));
        assertTrue("Shadow cell should be there",
                hasShadowCell(table, row, family, qualifier, t1.getStartTimestamp()));

        // Test that we can make a valid read after adding a shadow cell without hitting the commit table
        CommitTable.Client commitTableClient = spy(getTSO().getCommitTable().getClient().get());

        TSOClient client = TSOClient.newBuilder().withConfiguration(getTSO().getClientConfiguration())
                .build();
        TransactionManager tm2 = HBaseTransactionManager.newBuilder()
            .withConfiguration(hbaseConf).withTSOClient(client)
            .withCommitTableClient(commitTableClient).build();

        Transaction t2 = tm2.begin();
        Get get = new Get(row);
        get.addColumn(family, qualifier);

        Result getResult = table.get(t2, get);
        assertTrue("Values should be the same", Arrays.equals(data1, getResult.getValue(family, qualifier)));
        verify(commitTableClient, never()).getCommitTimestamp(anyLong());
    }

    @Test
    public void testCrashAfterCommit() throws Exception {
        CommitTable.Client commitTableClient = spy(getTSO().getCommitTable().getClient().get());

        TSOClient client = TSOClient.newBuilder().withConfiguration(getTSO().getClientConfiguration())
                .build();
        AbstractTransactionManager tm = spy((AbstractTransactionManager) HBaseTransactionManager.newBuilder()
                .withConfiguration(hbaseConf)
                .withCommitTableClient(commitTableClient)
                .withTSOClient(client).build());
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(tm).updateShadowCells(any(HBaseTransaction.class));

        TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        // Test shadow cell are created properly
        Put put = new Put(row);
        put.add(family, qualifier, data1);
        table.put(t1, put);
        try {
            tm.commit(t1);
        } catch (Exception e) { // (*) crash
            // Do nothing
        }

        assertTrue("Cell should be there",
                hasCell(table, row, family, qualifier, t1.getStartTimestamp()));
        assertFalse("Shadow cell should not be there",
                hasShadowCell(table, row, family, qualifier, t1.getStartTimestamp()));

        Transaction t2 = tm.begin();
        Get get = new Get(row);
        get.addColumn(family, qualifier);

        Result getResult = table.get(t2, get);
        assertTrue("Values should be the same", Arrays.equals(data1, getResult.getValue(family, qualifier)));
        verify(commitTableClient, times(1)).getCommitTimestamp(anyLong());
    }

    @Test
    public void testShadowCellIsHealedAfterCommitCrash() throws Exception {
        CommitTable.Client commitTableClient = spy(getTSO().getCommitTable().getClient().get());

        TSOClient client = TSOClient.newBuilder().withConfiguration(getTSO().getClientConfiguration())
                .build();
        AbstractTransactionManager tm = spy((AbstractTransactionManager) HBaseTransactionManager.newBuilder()
                .withConfiguration(hbaseConf)
                .withCommitTableClient(commitTableClient)
                .withTSOClient(client).build());
        // The following line emulates a crash after commit that is observed in (*) below
        doThrow(new RuntimeException()).when(tm).updateShadowCells(any(HBaseTransaction.class));

        TTable table = new TTable(hbaseConf, TEST_TABLE);

        HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        // Test shadow cell are created properly
        Put put = new Put(row);
        put.add(family, qualifier, data1);
        table.put(t1, put);
        try {
            tm.commit(t1);
        } catch (Exception e) { // (*) Crash
            // Do nothing
        }

        assertTrue("Cell should be there",
                hasCell(table, row, family, qualifier, t1.getStartTimestamp()));
        assertFalse("Shadow cell should not be there",
                hasShadowCell(table, row, family, qualifier, t1.getStartTimestamp()));

        Transaction t2 = tm.begin();
        Get get = new Get(row);
        get.addColumn(family, qualifier);

        // This get should heal the shadow cell
        Result getResult = table.get(t2, get);
        assertTrue("Values should be the same", Arrays.equals(data1, getResult.getValue(family, qualifier)));
        verify(commitTableClient, times(1)).getCommitTimestamp(anyLong());

        // Allow the healer thread to put the shadow cell back
        table.shadowCellHealerExecutor.submit(new Runnable() {
            @Override
            public void run() {

            }
        }).get();

        assertTrue("Cell should be there",
                hasCell(table, row, family, qualifier, t1.getStartTimestamp()));
        assertTrue("Shadow cell should be there after being healed",
                hasShadowCell(table, row, family, qualifier, t1.getStartTimestamp()));

        // As the shadow cell is healed, this get shouldn't have to hit the storage,
        // so the number of invocations to commitTableClient.getCommitTimestamp()
        // should remain the same
        getResult = table.get(t2, get);
        assertTrue("Values should be the same", Arrays.equals(data1, getResult.getValue(family, qualifier)));
        verify(commitTableClient, times(1)).getCommitTimestamp(anyLong());
    }

    @Test(timeout = 60000)
    public void testRaceConditionBetweenReaderAndWriterThreads() throws Exception {
        final CountDownLatch readAfterCommit = new CountDownLatch(1);
        final CountDownLatch postCommitBegin = new CountDownLatch(1);
        final CountDownLatch postCommitEnd = new CountDownLatch(1);

        final AtomicBoolean readFailed = new AtomicBoolean(false);
        AbstractTransactionManager tm = spy((AbstractTransactionManager) newTransactionManager());

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                LOG.info("Releasing readAfterCommit barrier");
                readAfterCommit.countDown();
                LOG.info("Waiting postCommitBegin barrier");
                postCommitBegin.await();
                invocation.callRealMethod();
                LOG.info("Releasing postCommitEnd barrier");
                postCommitEnd.countDown();
                return null;
            }
        }).when(tm).updateShadowCells(any(HBaseTransaction.class));

        // Start transaction on write thread
        TTable table = new TTable(hbaseConf, TEST_TABLE);

        final HBaseTransaction t1 = (HBaseTransaction) tm.begin();

        // Start read thread
        Thread readThread = new Thread("Read Thread") {
            @Override
            public void run() {
                LOG.info("Waiting readAfterCommit barrier");
                try {
                    readAfterCommit.await();
                    final TTable table = spy(new TTable(hbaseConf, TEST_TABLE));
                    doAnswer(new Answer<List<KeyValue>>() {
                        @SuppressWarnings("unchecked")
                        @Override
                        public List<KeyValue> answer(InvocationOnMock invocation) throws Throwable {
                            LOG.info("Release postCommitBegin barrier");
                            postCommitBegin.countDown();
                            LOG.info("Waiting postCommitEnd barrier");
                            postCommitEnd.await();
                            return (List<KeyValue>) invocation.callRealMethod();
                        }
                    }).when(table).filterKeyValuesForSnapshot(Matchers.<List<KeyValue>> any(),
                            any(HBaseTransaction.class), anyInt());

                    TransactionManager tm = newTransactionManager();
                    if (hasShadowCell(table, row, family, qualifier, t1.getStartTimestamp())) {
                        readFailed.set(true);
                    }

                    Transaction t = tm.begin();
                    Get get = new Get(row);
                    get.addColumn(family, qualifier);

                    Result getResult = table.get(t, get);
                    KeyValue kv = getResult.getColumnLatest(family, qualifier);
                    if (!Arrays.equals(data1, kv.getValue())
                            || !hasShadowCell(table, row, family, qualifier, kv.getTimestamp())) {
                        readFailed.set(true);
                    } else {
                        LOG.info("Read succeeded");
                    }
                } catch (Throwable e) {
                    readFailed.set(true);
                    LOG.error("Error whilst reading", e);
                }
            }
        };
        readThread.start();

        // Write data
        Put put = new Put(row);
        put.add(family, qualifier, data1);
        table.put(t1, put);
        tm.commit(t1);

        readThread.join();

        assertFalse("Read should have succeeded", readFailed.get());

    }

    public static boolean hasCell(TTable table, byte[] row, byte[] family, byte[] qualifier, long version)
            throws IOException {
        Get get = new Get(row);
        get.addColumn(family, qualifier);
        get.setTimeStamp(version);

        Result result = table.getHTable().get(get);
        return result.containsColumn(family, qualifier);
    }

    public static boolean hasShadowCell(TTable table, byte[] row, byte[] family, byte[] qualifier, long version)
            throws IOException {
        Get get = new Get(row);
        byte[] sc = HBaseTransactionManager.addShadowCellSuffix(qualifier);
        get.addColumn(family, sc);
        get.setTimeStamp(version);

        Result result = table.getHTable().get(get);
        return result.containsColumn(family, sc);
    }
}
