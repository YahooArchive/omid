package com.yahoo.omid.tsoclient;

import com.yahoo.omid.tso.util.DummyCellIdImpl;
import com.yahoo.omid.tsoclient.TSOClient.ConnectionException;
import com.yahoo.omid.tsoclient.TSOClientImpl.DisconnectedState;
import com.yahoo.statemachine.StateMachine.FsmImpl;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Sets.newHashSet;
import static com.yahoo.omid.tsoclient.TSOClient.TSO_HOST_CONFKEY;
import static com.yahoo.omid.tsoclient.TSOClient.TSO_PORT_CONFKEY;
import static com.yahoo.omid.tsoclient.TSOClient.TSO_RECONNECTION_DELAY_SECS;
import static com.yahoo.omid.tsoclient.TSOClient.ZK_CONNECTION_TIMEOUT_IN_SECS_CONFKEY;
import static org.slf4j.LoggerFactory.getLogger;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Test the behavior of requests on a TSOClient component that is not connected to a TSO server.
 */
public class TestUnconnectedTSOClient {

    private static final Logger LOG = getLogger(TestUnconnectedTSOClient.class);

    private static final int TSO_RECONNECTION_DELAY_IN_SECS_FOR_TEST = 2;

    @Test(timeOut = 30_000) // 30 secs
    public void testRequestsDoneOnAnUnconnectedTSOClientAlwaysReturn()
            throws Exception {
        Configuration clientConf = new BaseConfiguration();
        clientConf.setProperty(TSO_HOST_CONFKEY, "localhost");
        clientConf.setProperty(TSO_PORT_CONFKEY, 12345);
        clientConf.setProperty(TSO_RECONNECTION_DELAY_SECS, TSO_RECONNECTION_DELAY_IN_SECS_FOR_TEST);
        clientConf.setProperty(ZK_CONNECTION_TIMEOUT_IN_SECS_CONFKEY, 0); // Don't wait for ZK, it's not there

        // Component under test
        TSOClient tsoClient = TSOClient.newBuilder()
                .withConfiguration(clientConf)
                .build();

        // Internal accessor to fsm
        TSOClientImpl clientimpl = (TSOClientImpl) tsoClient;
        FsmImpl fsm = (FsmImpl) clientimpl.fsm;

        assertEquals(fsm.getState().getClass(), DisconnectedState.class);

        // Test requests to the 3 relevant methods in TSO client

        try {
            tsoClient.getNewStartTimestamp().get();
            fail();
        } catch (ExecutionException e) {
            LOG.info("Exception expected");
            assertEquals(e.getCause().getClass(), ConnectionException.class);
            TimeUnit.SECONDS.sleep(TSO_RECONNECTION_DELAY_IN_SECS_FOR_TEST * 2);
            assertEquals(fsm.getState().getClass(), DisconnectedState.class);
        }

        try {
            tsoClient.commit(1, newHashSet(new DummyCellIdImpl(0xdeadbeefL))).get();
            fail();
        } catch (ExecutionException e) {
            LOG.info("Exception expected");
            assertEquals(e.getCause().getClass(), ConnectionException.class);
            TimeUnit.SECONDS.sleep(TSO_RECONNECTION_DELAY_IN_SECS_FOR_TEST * 2);
            assertEquals(fsm.getState().getClass(), DisconnectedState.class);
        }

        tsoClient.close().get();
        LOG.info("No exception expected");
    }

}
