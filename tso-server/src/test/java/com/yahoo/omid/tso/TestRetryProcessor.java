package com.yahoo.omid.tso;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import org.testng.annotations.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import org.jboss.netty.channel.Channel;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.InMemoryCommitTable;

public class TestRetryProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TestRetryProcessor.class);

    private MetricRegistry metrics = new MetricRegistry();

    private static long NON_EXISTING_ST_TX = 1000;
    private static long ST_TX_1 = 0;
    private static long CT_TX_1 = 1;

    
    @Test(timeOut=10000)
    public void testBasicFunctionality() throws Exception {
        
        // Required mocks
        ReplyProcessor replyProc = mock(ReplyProcessor.class);
        Channel channel = Mockito.mock(Channel.class);
        
        CommitTable commitTable = new InMemoryCommitTable();
        commitTable.getWriter().get().addCommittedTransaction(ST_TX_1, CT_TX_1);
        
        // The element to test
        RetryProcessor retryProc = new RetryProcessorImpl(metrics, commitTable,
                                                          replyProc, new MockPanicker());

        // Test we'll reply with an abort for a retry request when the start timestamp IS NOT in the commit table 
        retryProc.disambiguateRetryRequestHeuristically(NON_EXISTING_ST_TX, channel);
        ArgumentCaptor<Long> firstTScapture = ArgumentCaptor.forClass(Long.class);
        verify(replyProc, timeout(100).times(1))
                        .abortResponse(firstTScapture.capture(), any(Channel.class));

        long startTS = firstTScapture.getValue();
        assertEquals("Captured timestamp should be the same as NON_EXISTING_ST_TX", NON_EXISTING_ST_TX, startTS);
        
        // Test we'll reply with a commit for a retry request when the start timestamp IS in the commit table 
        retryProc.disambiguateRetryRequestHeuristically(ST_TX_1, channel);
        ArgumentCaptor<Long> secondTScapture = ArgumentCaptor.forClass(Long.class);
        verify(replyProc, timeout(100).times(1))
                        .commitResponse(firstTScapture.capture(), secondTScapture.capture(), any(Channel.class));
        
        startTS = firstTScapture.getValue();
        long commitTS = secondTScapture.getValue();
        assertEquals("Captured timestamp should be the same as ST_TX_1", ST_TX_1, startTS);
        assertEquals("Captured timestamp should be the same as CT_TX_1", CT_TX_1, commitTS);
    }

}

