package com.yahoo.omid.tsoclient;

import static com.google.common.collect.Sets.newHashSet;
import static com.yahoo.omid.tsoclient.TSOClient.TSO_HOST_CONFKEY;
import static com.yahoo.omid.tsoclient.TSOClient.TSO_PORT_CONFKEY;
import static org.slf4j.LoggerFactory.getLogger;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import com.yahoo.omid.tso.util.DummyCellIdImpl;
import com.yahoo.omid.tsoclient.TSOClient.ConnectionException;
import com.yahoo.omid.tsoclient.TSOClientImpl.DisconnectedState;
import com.yahoo.statemachine.StateMachine.FsmImpl;

/**
 * Test the behavior of requests on a TSOClient component that is not
 * connected to a TSO server.
 */
public class TestUnconnectedTSOClient {

    private static final Logger LOG = getLogger(TestUnconnectedTSOClient.class);

    @Test(timeOut = 10_000) // 10 secs
    public void testRequestsDoneOnAnUnconnectedTSOClientAlwaysReturn()
    throws Exception {

        Configuration clientConf = new BaseConfiguration();
        clientConf.setProperty(TSO_HOST_CONFKEY, "localhost");
        clientConf.setProperty(TSO_PORT_CONFKEY, 12345);

        // Component under test
        TSOClient tsoClient = TSOClient.newBuilder()
                                       .withConfiguration(clientConf)
                                       .build();

        // Internal accessor to fsm
        TSOClientImpl clientimpl = (TSOClientImpl)tsoClient;
        FsmImpl fsm = (FsmImpl) clientimpl.fsm;

        // Test requests to the 3 relevant methods in TSO client

        try {
            tsoClient.getNewStartTimestamp().get();
            fail();
        } catch (ExecutionException e) {
            LOG.info("Exception expected");
            assertEquals(e.getCause().getClass(), ConnectionException.class);
            assertEquals(fsm.getState().getClass(), DisconnectedState.class);
        }

        try {
            tsoClient.commit(1, newHashSet(new DummyCellIdImpl(0xdeadbeefL))).get();
            fail();
        } catch (ExecutionException e) {
            LOG.info("Exception expected");
            assertEquals(e.getCause().getClass(), ConnectionException.class);
            assertEquals(fsm.getState().getClass(), DisconnectedState.class);
        }

        tsoClient.close().get();
        LOG.info("No exception expected");
        assertEquals(fsm.getState().getClass(), DisconnectedState.class);

    }

}
