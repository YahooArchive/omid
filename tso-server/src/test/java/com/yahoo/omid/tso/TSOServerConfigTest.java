package com.yahoo.omid.tso;

import org.testng.annotations.Test;

public class TSOServerConfigTest {

    @Test
    public void testParsesOK() throws Exception {
        new TSOServerConfig("test-omid.yml");
    }
}