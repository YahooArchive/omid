package com.yahoo.omid.tso;

import static org.testng.Assert.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.yahoo.omid.tso.TSOServer.BaseOperatingSystem;

public class TestTSOServer {

    private static final Logger LOG = LoggerFactory.getLogger(TestTSOServer.class);

    @Test
    public void testGetTheRightDefaultNetworkInterface() {

        String defaultNetworkIntf = TSOServer.getDefaultNetworkIntf();

        switch (BaseOperatingSystem.get()) {
        case Linux:
            LOG.info("We're in a Linux environment");
            assertEquals(defaultNetworkIntf, TSOServer.LINUX_TSO_NET_IFACE);
            break;
        case Mac:
            LOG.info("We're in a Mac environment");
            assertEquals(defaultNetworkIntf, TSOServer.MAC_TSO_NET_IFACE);
            break;
        }

    }

}
