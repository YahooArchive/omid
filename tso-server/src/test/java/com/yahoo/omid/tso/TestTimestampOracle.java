package com.yahoo.omid.tso;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

public class TestTimestampOracle {

    private static final Logger LOG = LoggerFactory.getLogger(TestTimestampOracle.class);
    
    MetricRegistry metrics = new MetricRegistry();

    @Test
    public void testMonotonicTimestampGrowth() throws Exception {
        TimestampOracle tso = new TimestampOracle(metrics, new TimestampOracle.InMemoryTimestampStorage());
        long last = tso.next();
        for (int i = 0; i < (3 * TimestampOracle.TIMESTAMP_BATCH); i++) {
            long current = tso.next();
            assertEquals("Not monotonic growth", last + 1, current);
            last = current;
        }
        LOG.info("Last timestamp: {}", last);
    }

}
