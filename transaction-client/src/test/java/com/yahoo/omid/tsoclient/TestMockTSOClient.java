package com.yahoo.omid.tsoclient;

import com.google.common.collect.Sets;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.InMemoryCommitTable;
import com.yahoo.omid.tso.util.DummyCellIdImpl;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

import static org.testng.AssertJUnit.assertEquals;

public class TestMockTSOClient {

    final static public CellId c1 = new DummyCellIdImpl(0xdeadbeefL);
    final static public CellId c2 = new DummyCellIdImpl(-0xfeedcafeL);

    @Test(timeOut = 10000)
    public void testConflicts() throws Exception {
        CommitTable commitTable = new InMemoryCommitTable();
        TSOProtocol client = new MockTSOClient(commitTable.getWriter());

        long tr1 = client.getNewStartTimestamp().get();
        long tr2 = client.getNewStartTimestamp().get();

        client.commit(tr1, Sets.newHashSet(c1)).get();

        try {
            client.commit(tr2, Sets.newHashSet(c1, c2)).get();
            Assert.fail("Shouldn't have committed");
        } catch (ExecutionException ee) {
            assertEquals("Should have aborted", ee.getCause().getClass(), AbortException.class);
        }
    }

    @Test(timeOut = 10000)
    public void testWatermarkUpdate() throws Exception {
        CommitTable commitTable = new InMemoryCommitTable();
        TSOProtocol client = new MockTSOClient(commitTable.getWriter());
        CommitTable.Client commitTableClient = commitTable.getClient();

        long tr1 = client.getNewStartTimestamp().get();
        client.commit(tr1, Sets.newHashSet(c1)).get();

        long initWatermark = commitTableClient.readLowWatermark().get();

        long tr2 = client.getNewStartTimestamp().get();
        client.commit(tr2, Sets.newHashSet(c1)).get();

        long newWatermark = commitTableClient.readLowWatermark().get();
        AssertJUnit.assertTrue("new low watermark should be bigger", newWatermark > initWatermark);
    }
}
