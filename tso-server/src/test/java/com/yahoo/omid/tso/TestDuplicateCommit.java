package com.yahoo.omid.tso;

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.yahoo.omid.proto.TSOProto;
import com.yahoo.omid.proto.TSOProto.Response;
import com.yahoo.omid.tsoclient.CellId;

public class TestDuplicateCommit extends TSOTestBase {
    
    Set<CellId> cells = Sets.newHashSet(c1, c2);
    
    TSOClientOneShot clientOneShot = null; 
    
    @Before
    public void createClient() {
        clientOneShot = new TSOClientOneShot(clientConf.getString("tso.host"), clientConf.getInt("tso.port"));
    }
    
    /**
     * This tests the case where messages arrive at the TSO out of order. This can happen in the case
     * the channel get dropped and the retry is done in a new channel. However, the TSO will respond with
     * aborted to the original message because the retry was already committed and it would be prohibitively
     * expensive to check all non-retry requests to see if they are already committed. For this reason
     * a client must ensure that if it is sending a retry due to a socket error, the previous channel
     * must be entirely closed so that it will not actually receive the abort response. TCP guarantees
     * that this doesn't happen in non-socket error cases.
     * 
     */
    @Test
    public void testOutOfOrderMessages() throws Exception {
        long ts1 = client.createTransaction().get();
        
        Response response1 = clientOneShot.makeRequest(createCommitRequest(ts1, true));
        Response response2 = clientOneShot.makeRequest(createCommitRequest(ts1, false));
        assertFalse("Retry Transaction should commit", response1.getCommitResponse().getAborted());
        assertTrue("Transaction should abort", response2.getCommitResponse().getAborted());        
    }
    
    @Test
    public void testDuplicateCommitAborting() throws Exception {

        long ts1 = client.createTransaction().get();
        long ts2 = client.createTransaction().get();
        client.commit(ts2, cells).get();
                        
        Response response1 = clientOneShot.makeRequest(createCommitRequest(ts1, false));
        Response response2 = clientOneShot.makeRequest(createCommitRequest(ts1, true));
        assertTrue("Transaction should abort", response1.getCommitResponse().getAborted());
        assertTrue("Retry commit should abort", response2.getCommitResponse().getAborted());
    }
    
    @Test
    public void testDuplicateCommit() throws Exception {
        
        long ts1 = client.createTransaction().get();
        
        Response response1 = clientOneShot.makeRequest(createCommitRequest(ts1, false));
        Response response2 = clientOneShot.makeRequest(createCommitRequest(ts1, true));
        assertEquals("Commit timestamp should be the same",
                response1.getCommitResponse().getCommitTimestamp(), response2.getCommitResponse().getCommitTimestamp());
    }

    private TSOProto.Request createCommitRequest(long ts, boolean retry) {
        TSOProto.Request.Builder builder = TSOProto.Request.newBuilder();
        TSOProto.CommitRequest.Builder commitBuilder = TSOProto.CommitRequest.newBuilder();
        commitBuilder.setStartTimestamp(ts);
        commitBuilder.setIsRetry(retry);
        for (CellId cell : cells) {
            commitBuilder.addCellId(cell.getCellId());
        }
        return builder.setCommitRequest(commitBuilder.build()).build();
    }
    
}
