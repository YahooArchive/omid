package com.yahoo.omid.tso;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class TestUncommitted {

    private static final Logger logger = LoggerFactory.getLogger(TestUncommitted.class);

    @Test
    public void testRandomOperations() {
        final int iterations = 1000;
        final int operations = 100000;
        final double raiseLargest = 0.05;
        final double commit = 0.9; 
        for (int i = 0; i < iterations; ++i) {
            long seed = System.nanoTime();
//            seed = 1371038396995781000L;
            Random rand = new Random(seed);
            logger.trace("Random seed: {}", seed);
            
            long startTimestamp = Math.abs(rand.nextLong()) % (Long.MAX_VALUE >> 1);
//            startTimestamp %= 4*32;
            long currentTimestamp = startTimestamp;
            long largestDeleted = startTimestamp;
            
            Uncommitted uncommitted = new Uncommitted(startTimestamp, 4, 32);
            Set<Long> toAbort = new HashSet<Long>();
            // The latest uncommitted have to be ignored at the end
            Set<Long> latestUncommitted = new HashSet<Long>();
            
            for (int j = 0; j < operations; ++j) {
                long diff = currentTimestamp - largestDeleted;
                double op = rand.nextDouble();
                if (diff > 90) {
                    op = 0; // raise largest
                }

                if (op < raiseLargest) {
                    // raise
                    if (diff == 0) {
                        continue;
                    }
                    largestDeleted += rand.nextInt((int) diff);
                    Set<Long> aborted = uncommitted.raiseLargestDeletedTransaction(largestDeleted);
                    
                    Set<Long> abortDiff = Sets.difference(aborted, toAbort);
                    
                    assertTrue("Aborted transactions that weren't uncommited, seed " + seed + " j " + j + " diff "
                            + abortDiff, toAbort.containsAll(aborted));
                    toAbort.removeAll(aborted);
                } else if (op < commit) {
                    // commit
                    currentTimestamp++;
                    uncommitted.commit(currentTimestamp);
                    latestUncommitted.clear();
                } else {
                    // uncommitted
                    currentTimestamp++;
                    toAbort.add(currentTimestamp);
                    latestUncommitted.add(currentTimestamp);
                }
            }
            Set<Long> aborted = uncommitted.raiseLargestDeletedTransaction(currentTimestamp);
            Set<Long> abortDiff = Sets.difference(aborted, toAbort);
            assertTrue("Aborted transactions that weren't uncommited, seed " + seed + " diff " + abortDiff,
                    toAbort.containsAll(aborted));
            toAbort.removeAll(aborted);
            toAbort.removeAll(latestUncommitted);
            assertTrue("toAbort is not empty, seed " + seed, toAbort.isEmpty());
        }
    }
    
}
