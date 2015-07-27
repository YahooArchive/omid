package com.yahoo.omid.tso;

/**
 * This interface defines a container for long-long mappings, for instance for holding row -> last commit information
 *
 */
public interface Cache {

    public void reset();

    public abstract long set(long key, long value);

    public abstract long get(long key);

}