package com.yahoo.omid.tso;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.ExecutionException;

import org.testng.annotations.Test;

import com.yahoo.omid.proto.TSOProto;
import com.yahoo.omid.tsoclient.TSOClient;

public class TestHandshake extends TSOTestBase {
    /**
     * Test that if a client tries to make a request
     * without handshaking, it will be disconnected.
     */
    @Test(timeOut=10000)
    public void testOldClientCurrentServer() throws Exception {
        TSOClientRaw raw = new TSOClientRaw(clientConf.getString("tso.host"),
                                            clientConf.getInt("tso.port"));

        TSOProto.Request request = TSOProto.Request.newBuilder()
            .setTimestampRequest(TSOProto.TimestampRequest.newBuilder().build())
            .build();
        raw.write(request);
        try {
            raw.getResponse().get();
            fail("Channel should close");
        } catch (ExecutionException ee) {
            assertEquals("Should be channel closed exception",
                    TSOClient.ConnectionException.class, ee.getCause().getClass());
        }
        raw.close();
   }
}
