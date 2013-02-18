package com.yahoo.omid.tso;

public class LongCache {

    private final long [] cache;
    private final int size;
    private final int associativity;
    
    public LongCache(int size, int associativity) {
        this.size = size;
        this.cache = new long[2*(size + associativity)];
        this.associativity = associativity;
    }
    
    public long set(long key, long value) {
        final int index = index(key);
        int oldestIndex = 0;
        long oldestValue = Long.MAX_VALUE;
        for (int i = 0; i < associativity; ++i) {
            int currIndex = 2 * (index + i);
            if (cache[currIndex + 1] <= oldestValue) {
                oldestValue = cache[currIndex + 1];
                oldestIndex = currIndex;
            }
        }
        cache[oldestIndex] = key;
        cache[oldestIndex + 1] = value;
        return oldestValue;
    }
    
    public long get(long key) {
        final int index = index(key);
        for (int i = 0; i < associativity; ++i) {
            int currIndex = 2 * (index + i);
            if (cache[currIndex] == key) {
                return cache[currIndex + 1];
            }
        }
        return 0;
    }

    private int index(long hash) {
        return (int) (Math.abs(hash) % size);
    }

}
