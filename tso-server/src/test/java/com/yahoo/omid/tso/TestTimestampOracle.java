package com.yahoo.omid.tso;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

public class TestTimestampOracle {

    private static final Logger LOG = LoggerFactory.getLogger(TestTimestampOracle.class);
    
    MetricRegistry metrics = new MetricRegistry();

    @Test
    public void testMonotonicTimestampGrowth() throws Exception {
        TimestampOracleImpl tso = new TimestampOracleImpl(metrics,
                new TimestampOracleImpl.InMemoryTimestampStorage(), new MockPanicker());
        long last = tso.next();
        for (int i = 0; i < (3 * TimestampOracleImpl.TIMESTAMP_BATCH); i++) {
            long current = tso.next();
            AssertJUnit.assertEquals("Not monotonic growth", last + 1, current);
            last = current;
        }
        LOG.info("Last timestamp: {}", last);
    }

}
