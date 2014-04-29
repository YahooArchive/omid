package com.yahoo.omid.tso;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

public class TestTimestampOracle {

    MetricRegistry metrics = new MetricRegistry();

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testMonotonicTimestampGrowth() throws Exception {
        TimestampOracle tso = new TimestampOracle(metrics, new TimestampOracle.InMemoryTimestampStorage());
        long last = tso.next();
        for (int i = 0; i < 1000000; i++) {
            long current = tso.next();
            assertEquals("Not monotonic growth", last + 1, current);
            last = current;
        }
    }

}
