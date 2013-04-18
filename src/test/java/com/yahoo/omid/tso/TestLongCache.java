package com.yahoo.omid.tso;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertThat;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class TestLongCache {

    private static final Log LOG = LogFactory.getLog(TestLongCache.class);

    final int entries = 1000;
    Histogram hist = new Histogram(entries * 10);

    @Test
    public void testEntriesAge() {
        

        Cache cache = new LongCache(entries, 16);
        Random random = new Random();

        long seed = random.nextLong();

        LOG.info("Random seed: " + seed);
        random.setSeed(seed);
        int removals = 0;
        long totalAge = 0;
        double tempStdDev = 0;
        double tempAvg = 0;

        int i = 0;
        int largestDeletedTimestamp = 0;
        for (; i < entries * 10; ++i) {
            long removed = cache.set(random.nextLong(), i);
            if (removed > largestDeletedTimestamp) {
                largestDeletedTimestamp = (int) removed;
            }
        }

        long time = System.nanoTime();
        for (; i < entries * 100; ++i) {
            long removed = cache.set(random.nextLong(), i);
            if (removed > largestDeletedTimestamp) {
                largestDeletedTimestamp = (int) removed;
            }
            int gap = i - ((int) largestDeletedTimestamp);
            removals++;
            totalAge += gap;
            double oldAvg = tempAvg;
            tempAvg += (gap - tempAvg) / removals;
            tempStdDev += (gap - oldAvg) * (gap - tempAvg);
            hist.add(gap);
        }
        long elapsed = System.nanoTime() - time;
        LOG.info("Elapsed (ms): " + (elapsed / (double)1000));

        double avgGap = totalAge / (double) removals;
        LOG.info("Avg gap: " + (tempAvg ));
        LOG.info("Std dev gap: " + Math.sqrt((tempStdDev / entries)));
        assertThat(avgGap, is(greaterThan(entries * .6 )));
    }
}
