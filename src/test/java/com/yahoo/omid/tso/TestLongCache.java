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

    @Test
    public void testEntriesAge() {
        final int entries = 10;
        
        Histogram hist = new Histogram(entries * 10);

        LongCache cache = new LongCache(entries, 1);
        Random random = new Random();

        long seed = random.nextLong();

        LOG.info("Random seed: " + seed);
        random.setSeed(seed);
        int removals = 0;
        long totalAge = 0;
        double tempStdDev = 0;
        double tempAvg = 0;
        
        int i = 0;
        for (; i < entries * 10; ++i) {
            cache.set(random.nextLong(), i);
        }

        for (; i < entries * 100; ++i) {
            long removed = cache.set(random.nextLong(), i);
            int age = i - ((int) removed);
            removals++;
            totalAge += age;
            double oldAvg = tempAvg;
            tempAvg += (age - tempAvg) / removals;
            tempStdDev += (age - oldAvg) * (age - tempAvg);
            hist.add(age);
        }

        double avgAge = totalAge / (double) removals;
        LOG.info("Avg age: " + (avgAge ));
        LOG.info("Avg age: " + (tempAvg ));
        LOG.info("Std dev age: " + Math.sqrt((tempStdDev / entries)));
        System.out.println(hist.toString());
        assertThat(avgAge, is(greaterThan(entries * .9 )));
    }
}
