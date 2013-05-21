package com.yahoo.omid.tso;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.notifications.client.ObserverWrapper;

public class TestUncommitted {

    private static final Logger logger = LoggerFactory.getLogger(ObserverWrapper.class);

    @Test
    public void testRandomOperations() {
        final int iterations = 1000;
        final int operations = 100000;
        final double raiseLargest = 0.05;
        final double commit = 0.9; 
        for (int i = 0; i < iterations; ++i) {
            long seed = System.nanoTime();
            Random rand = new Random(seed);
            logger.info("Random seed: {}", seed);
            
            final long startTimestamp = Math.abs(rand.nextLong()) % (Long.MAX_VALUE >> 1);
            long currentTimestamp = startTimestamp;
            long largestDeleted = startTimestamp;
            
            Uncommitted uncommitted = new Uncommitted(startTimestamp);
            Set<Long> toAbort = new HashSet<Long>();
            
            for (int j = 0; j < operations; ++j) {
                double op = rand.nextDouble();
                if (op < raiseLargest) {
                    // raise
                    long diff = currentTimestamp - largestDeleted;
                    if (diff == 0) {
                        continue;
                    }
                    largestDeleted += rand.nextInt((int) diff);
                    Set<Long> aborted = uncommitted.raiseLargestDeletedTransaction(largestDeleted);
                    
                    assertTrue("Aborted transactions that weren't uncommited", toAbort.containsAll(aborted));
                    toAbort.removeAll(aborted);
                } else if (op < commit) {
                    // commit
                    currentTimestamp++;
                    uncommitted.commit(currentTimestamp);
                } else {
                    // uncommitted
                    currentTimestamp++;
                    toAbort.add(currentTimestamp);
                }
            }
            Set<Long> aborted = uncommitted.raiseLargestDeletedTransaction(currentTimestamp);
            assertTrue("Aborted transactions that weren't uncommited", toAbort.containsAll(aborted));
            toAbort.removeAll(aborted);
            assertTrue("toAbort is not empty", toAbort.isEmpty());
        }
    }
    
}
