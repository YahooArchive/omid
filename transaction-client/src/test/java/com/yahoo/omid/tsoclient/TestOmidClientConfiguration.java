package com.yahoo.omid.tsoclient;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestOmidClientConfiguration {

    @Test
    public void testYamlReading() {
        OmidClientConfiguration configuration = new OmidClientConfiguration();
        Assert.assertNotNull(configuration.getConnectionString());
        Assert.assertNotNull(configuration.getConnectionType());
    }

}