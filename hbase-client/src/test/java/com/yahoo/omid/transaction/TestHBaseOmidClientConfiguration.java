package com.yahoo.omid.transaction;

import org.junit.Assert;
import org.testng.annotations.Test;

public class TestHBaseOmidClientConfiguration {

    @Test
    public void testYamlReading() {
        HBaseOmidClientConfiguration configuration = new HBaseOmidClientConfiguration();
        Assert.assertNotNull(configuration.getCommitTableName());
        Assert.assertNotNull(configuration.getHBaseConfiguration());
        Assert.assertNotNull(configuration.getMetrics());
        Assert.assertNotNull(configuration.getOmidClientConfiguration());
    }

    @Test
    public void testYamlReadingFromFile() {
        HBaseOmidClientConfiguration configuration = new HBaseOmidClientConfiguration("/test-hbase-omid-client-config.yml");
        Assert.assertNotNull(configuration.getCommitTableName());
        Assert.assertNotNull(configuration.getHBaseConfiguration());
        Assert.assertNotNull(configuration.getMetrics());
        Assert.assertNotNull(configuration.getOmidClientConfiguration());
    }

}