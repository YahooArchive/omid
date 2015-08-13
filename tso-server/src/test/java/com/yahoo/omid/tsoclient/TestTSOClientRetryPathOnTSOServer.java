package com.yahoo.omid.tsoclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.yahoo.omid.proto.TSOProto;
import com.yahoo.omid.proto.TSOProto.Response;
import com.yahoo.omid.tso.TSOClientOneShot;
import com.yahoo.omid.tso.TSOTestBase;

/**
 * These tests are similar to the ones in TestRetryProcessor but checking the
 * result on the TSOClient side (They exercise the communication protocol)
 * TODO Remove???
 */
public class TestTSOClientRetryPathOnTSOServer extends TSOTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestTSOClientRetryPathOnTSOServer.class);

    TSOClientOneShot clientOneShot;

    @BeforeMethod
    public void createClient() {
        clientOneShot = new TSOClientOneShot(clientConf.getString("tso.host"), clientConf.getInt("tso.port"));
    }

    @Test
    public void testCommitTimestampPresentInCommitTableReturnsCommit() throws Exception {

        long tx1ST = client.getNewStartTimestamp().get();

        clientOneShot.makeRequest(createRetryCommitRequest(tx1ST));
        Response response = clientOneShot.makeRequest(createRetryCommitRequest(tx1ST));
        assertFalse(response.getCommitResponse().getAborted(), "Transaction should be committed");
        assertFalse(response.getCommitResponse().getMakeHeuristicDecision());
        assertEquals(response.getCommitResponse().getCommitTimestamp(), 2);
    }

    @Test
    public void testInvalidCommitTimestampPresentInCommitTableReturnsAbort() throws Exception {

        long tx1ST = client.getNewStartTimestamp().get();
        // Invalidate the transaction
        getCommitTable().getClient().get().tryInvalidateTransaction(tx1ST);

        clientOneShot.makeRequest(createRetryCommitRequest(tx1ST));
        Response response = clientOneShot.makeRequest(createRetryCommitRequest(tx1ST));
        assertTrue(response.getCommitResponse().getAborted(), "Transaction should be aborted");
        assertFalse(response.getCommitResponse().getMakeHeuristicDecision());
        assertEquals(response.getCommitResponse().getCommitTimestamp(), 0);
    }

    @Test
    public void testCommitTimestampNotPresentInCommitTableReturnsAnAbort() throws Exception {

        long tx1ST = client.getNewStartTimestamp().get();

        clientOneShot.makeRequest(createRetryCommitRequest(tx1ST));

        // Simulate remove entry from the commit table before exercise retry
        getCommitTable().getClient().get().completeTransaction(tx1ST);

        Response response = clientOneShot.makeRequest(createRetryCommitRequest(tx1ST));
        assertTrue(response.getCommitResponse().getAborted(), "Transaction should abort");
        assertFalse(response.getCommitResponse().getMakeHeuristicDecision());
        assertEquals(response.getCommitResponse().getCommitTimestamp(), 0);
    }

    // ********************************************************************
    // Helper methods
    // ********************************************************************

    private TSOProto.Request createRetryCommitRequest(long ts) {
        TSOProto.Request.Builder builder = TSOProto.Request.newBuilder();
        TSOProto.CommitRequest.Builder commitBuilder = TSOProto.CommitRequest.newBuilder();
        commitBuilder.setStartTimestamp(ts);
        commitBuilder.setIsRetry(true);
        for (CellId cell : Sets.newHashSet(c1, c2)) {
            commitBuilder.addCellId(cell.getCellId());
        }
        return builder.setCommitRequest(commitBuilder.build()).build();
    }

}
