package com.yahoo.omid.tso;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

public class CacheEvaluation {

    final int entries = 1000000;
    Histogram hist = new Histogram(entries * 100);

    // @Test
    public void testEntriesAge(int asoc, PrintWriter writer) {

        LongCache cache = new LongCache(entries, asoc);
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
            int gap = i - largestDeletedTimestamp;
            removals++;
            totalAge += gap;
            double oldAvg = tempAvg;
            tempAvg += (gap - tempAvg) / removals;
            tempStdDev += (gap - oldAvg) * (gap - tempAvg);
            hist.add(gap);
        }
        long elapsed = System.nanoTime() - time;
        double elapsedSeconds =  (elapsed / (double) 1000000000);
        writer.println("# Elapsed (s): " + elapsedSeconds);
        writer.println("# Elapsed per 100 ops (ms): " + (elapsed / (double) entries / (double) 1000000));
        writer.println("# Ops per s : " + ((entries * 100) / elapsedSeconds));

        double avgAge = totalAge / (double) removals;
        // LOG.info("Avg gap: " + (avgAge ));
        writer.println("# Avg gap: " + (tempAvg));
        writer.println("# Std dev gap: " + Math.sqrt((tempStdDev / entries)));
        hist.print(writer);
        // System.out.println(hist.toString());
        // assertThat(avgAge, is(greaterThan(entries * .9 )));
    }

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        int[] asoc = new int[] { 1, 2, 4, 8, 16, 32 };
        for (int i = 0; i < asoc.length; ++i) {
            PrintWriter writer = new PrintWriter(asoc[i] + ".txt", "UTF-8");
            new CacheEvaluation().testEntriesAge(asoc[i], writer);
            writer.close();
        }
    }
}
