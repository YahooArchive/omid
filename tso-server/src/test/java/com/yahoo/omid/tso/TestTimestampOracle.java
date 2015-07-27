package com.yahoo.omid.tso;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.omid.metrics.MetricsRegistry;
import com.yahoo.omid.timestamp.storage.TimestampStorage;

public class TestTimestampOracle {

    private static final Logger LOG = LoggerFactory.getLogger(TestTimestampOracle.class);

    @Mock
    private MetricsRegistry metrics;
    @Mock
    private Panicker panicker;
    @Mock
    private TimestampStorage timestampStorage;

    // Component under test
    @InjectMocks
    private TimestampOracleImpl tso;

    @BeforeMethod(alwaysRun = true)
    public void initMocksAndComponents() {

        MockitoAnnotations.initMocks(this);

    }

    @Test
    public void testMonotonicTimestampGrowth() throws Exception {

        // Intialize component under test
        tso.initialize();

        long last = tso.next();
        for (int i = 0; i < (3 * TimestampOracleImpl.TIMESTAMP_BATCH); i++) {
            long current = tso.next();
            assertEquals(current, last + 1, "Not monotonic growth");
            last = current;
        }
        assertTrue(tso.getLast() == last);
        LOG.info("Last timestamp: {}", last);
    }

    @Test
    public void testTimestampOraclePanic() throws Exception {

        // Intialize component under test
        tso.initialize();

        // Cause an exception when updating the max timestamp
        doThrow(new RuntimeException("Out of memory or something"))
            .when(timestampStorage).updateMaxTimestamp(anyLong(), anyLong());

        // Make the previous exception to be thrown
        Thread allocThread = new Thread("AllocThread") {
            @Override
            public void run() {
                try {
                    while (true) {
                        tso.next();
                    }
                } catch (IOException ioe) {
                    LOG.error("Shouldn't occur");
                }
            }
        };
        allocThread.start();

        // Verify that it has blown up
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));
    }
}
