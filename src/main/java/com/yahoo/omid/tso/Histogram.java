package com.yahoo.omid.tso;

import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Histogram {
    private static final Log LOG = LogFactory.getLog(Histogram.class);
    final private int size;
    final private int[] counts;
    private int max;

    public Histogram(int size) {
        this.size = size;
        this.counts = new int[size];
    }

    public void add(int i) {
        if (i >= size) {
            LOG.error("Tried to add " + i + " which is bigger than size " + size);
            return;
        }
        counts[i]++;
        if (i > max) {
            max = i;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(max).append('\n');
        for (int i = 0; i <= max; ++i) {
            sb.append("[").append(i).append("]\t");
        }
        sb.append('\n');
        for (int i = 0; i <= max; ++i) {
            sb.append(counts[i]).append("\t");
        }
        return sb.toString();
    }

    public void log() {
        for (int i = 0; i <= max; ++i) {
            LOG.debug(String.format("[%5d]\t%5d", i, counts[i]));
        }
    }

    public void print(PrintWriter writer) {
        for (int i = 0; i <= max; ++i) {
            writer.format("%5d\t%5d\n", i, counts[i]);
        }
    }
}
