package com.yahoo.omid.tso;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.proto.TSOProto;
import com.yahoo.omid.tsoclient.TSOClient.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestHandshake  {

    private static final Logger LOG = LoggerFactory.getLogger(TestHandshake.class);

    private final int TSO_SERVER_PORT = 1234;

    // Component under test
    private TSOServer tso;

    @BeforeClass
    public void setup() throws Exception {

        Module tsoServerMockModule = new TSOMockModule(TSOServerCommandLineConfig.configFactory(TSO_SERVER_PORT, 1000));
        Injector injector = Guice.createInjector(tsoServerMockModule);

        LOG.info("==================================================================================================");
        LOG.info("======================================= Init TSO Server ==========================================");
        LOG.info("==================================================================================================");

        tso = injector.getInstance(TSOServer.class);
        tso.startAndWait();
        TestUtils.waitForSocketListening("localhost", TSO_SERVER_PORT, 100);

        LOG.info("==================================================================================================");
        LOG.info("===================================== TSO Server Initialized =====================================");
        LOG.info("==================================================================================================");
    }

    /**
     * Test that if a client tries to make a request without handshaking, it will be disconnected.
     */
    @Test(timeOut = 10_000)
    public void testOldClientCurrentServer() throws Exception {

        TSOClientRaw raw = new TSOClientRaw("localhost", TSO_SERVER_PORT);

        TSOProto.Request request = TSOProto.Request.newBuilder()
            .setTimestampRequest(TSOProto.TimestampRequest.newBuilder().build())
            .build();
        raw.write(request);
        try {
            raw.getResponse().get();
            fail("Channel should be closed");
        } catch (ExecutionException ee) {
            assertEquals(ee.getCause().getClass(), ConnectionException.class, "Should be channel closed exception");
        }
        raw.close();

   }

}
