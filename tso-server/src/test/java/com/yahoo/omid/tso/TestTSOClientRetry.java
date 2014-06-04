package com.yahoo.omid.tso;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import com.yahoo.omid.committable.CommitTable.Client;
import com.yahoo.omid.tsoclient.TSOClient;
import com.yahoo.omid.tsoclient.TSOClientAccessor;
import com.yahoo.omid.tsoclient.TSOFuture;
import com.yahoo.omid.tsoclient.TSOClient.ServiceUnavailableException;

public class TestTSOClientRetry extends TSOTestBase {

    @Test(timeout = 60000)
    public void testCommitCanSucceedWhenChannelDisconnected() throws Exception {
        
        long ts1 = client.getNewStartTimestamp().get();
        pauseTSO();
        TSOFuture<Long> future = client.commit(ts1, Sets.newSet(c1, c2));
        while(!isTsoBlockingRequest()) {}
        TSOClientAccessor.closeChannel(client);
        resumeTSO();
        future.get();
        
    }

    @Test(timeout = 60000)
    public void testCommitCanSucceedWithMultipleTimeouts() throws Exception {
        Configuration clientConfiguration = getClientConfiguration();
        clientConfiguration.setProperty(TSOClient.REQUEST_TIMEOUT_IN_MS_CONFKEY, 100);
        clientConfiguration.setProperty(TSOClient.REQUEST_MAX_RETRIES_CONFKEY, 10000);
        
        TSOClient client = TSOClient.newBuilder().withConfiguration(clientConf).build();
        
        long ts1 = client.getNewStartTimestamp().get();
        pauseTSO();
        TSOFuture<Long> future = client.commit(ts1, Sets.newSet(c1, c2));
        while(!isTsoBlockingRequest()) {}
        Thread.sleep(1000);
        resumeTSO();
        future.get();
    }
    
    @Test(timeout = 60000)
    public void testCommitFailWhenTSOIsDown() throws Exception {
        Configuration clientConfiguration = getClientConfiguration();
        clientConfiguration.setProperty(TSOClient.REQUEST_TIMEOUT_IN_MS_CONFKEY, 100);
        clientConfiguration.setProperty(TSOClient.REQUEST_MAX_RETRIES_CONFKEY, 10);
        
        TSOClient client = TSOClient.newBuilder().withConfiguration(clientConf).build();
        
        long ts1 = client.getNewStartTimestamp().get();
        pauseTSO();
        TSOFuture<Long> future = client.commit(ts1, Sets.newSet(c1, c2));
        while(!isTsoBlockingRequest()) {}
        try {
            future.get();
        } catch(ExecutionException e) {
            assertEquals("Should be a ServiceUnavailableExeption",
                    ServiceUnavailableException.class, e.getCause().getClass());
        }
    }
    
    @Test(timeout = 60000)
    public void testTimestampRequestSucceedWithMultipleTimeouts() throws Exception {
        Configuration clientConfiguration = getClientConfiguration();
        clientConfiguration.setProperty(TSOClient.REQUEST_TIMEOUT_IN_MS_CONFKEY, 100);
        clientConfiguration.setProperty(TSOClient.REQUEST_MAX_RETRIES_CONFKEY, 10000);
        
        TSOClient client = TSOClient.newBuilder().withConfiguration(clientConf).build();
        
        pauseTSO();
        Future<Long> future = client.getNewStartTimestamp();
        while(!isTsoBlockingRequest()) {}
        Thread.sleep(1000);
        resumeTSO();
        future.get();
    }
}

