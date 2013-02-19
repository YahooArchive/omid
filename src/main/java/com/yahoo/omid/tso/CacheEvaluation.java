package com.yahoo.omid.tso;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

public class CacheEvaluation {

    final static int ENTRIES = 1000000;
    final static int WARMUP_ROUNDS = 20;
    final static int ROUNDS = 20;
    Histogram hist = new Histogram(ENTRIES * 100);

    public void testEntriesAge(int asoc, PrintWriter writer) {

        LongCache cache = new LongCache(ENTRIES, asoc);
        Random random = new Random();

        long seed = random.nextLong();

        writer.println("# Random seed: " + seed);
        random.setSeed(seed);
        int removals = 0;
        long totalAge = 0;
        double tempStdDev = 0;
        double tempAvg = 0;

        int i = 0;
        int largestDeletedTimestamp = 0;
        for (; i < ENTRIES * WARMUP_ROUNDS; ++i) {
            long removed = cache.set(random.nextLong(), i);
            if (removed > largestDeletedTimestamp) {
                largestDeletedTimestamp = (int) removed;
            }
            if (i % ENTRIES == 0) {
                int round = i / ENTRIES + 1;
                System.err.format("Warmup [%d/%d]\n", round, WARMUP_ROUNDS);
            }
        }

        long time = System.nanoTime();
        for (; i < ENTRIES * (WARMUP_ROUNDS + ROUNDS); ++i) {
            long removed = cache.set(random.nextLong(), i);
            if (removed > largestDeletedTimestamp) {
                largestDeletedTimestamp = (int) removed;
            }
            int gap = i - largestDeletedTimestamp;
            removals++;
            totalAge += gap;
            double oldAvg = tempAvg;
            tempAvg += (gap - tempAvg) / removals;
            tempStdDev += (gap - oldAvg) * (gap - tempAvg);
            hist.add(gap);
            if (i % ENTRIES == 0) {
                int round = i / ENTRIES - WARMUP_ROUNDS + 1;
                System.err.format("Progress [%d/%d]\n", round, ROUNDS);
            }
        }
        long elapsed = System.nanoTime() - time;
        double elapsedSeconds = (elapsed / (double) 1000000000);
        long totalOps = ENTRIES * ROUNDS;
        writer.println("# Elapsed (s): " + elapsedSeconds);
        writer.println("# Elapsed per 100 ops (ms): " + (elapsed / (double) totalOps / 100 / (double) 1000000));
        writer.println("# Ops per s : " + (totalOps / elapsedSeconds));

        double avgAge = totalAge / (double) removals;
        // LOG.info("Avg gap: " + (avgAge ));
        writer.println("# Avg gap: " + (tempAvg));
        writer.println("# Std dev gap: " + Math.sqrt((tempStdDev / ENTRIES)));
        hist.print(writer);
        // System.out.println(hist.toString());
        // assertThat(avgAge, is(greaterThan(entries * .9 )));
    }

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        int[] asoc = new int[] { 1, 2, 4, 8, 16, 32 };
        for (int i = 0; i < asoc.length; ++i) {
            PrintWriter writer = new PrintWriter(asoc[i] + ".out", "UTF-8");
            new CacheEvaluation().testEntriesAge(asoc[i], writer);
            writer.close();
        }
    }
}
