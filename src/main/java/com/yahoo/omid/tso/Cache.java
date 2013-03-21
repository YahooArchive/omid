package com.yahoo.omid.tso;

public interface Cache {

    public abstract long set(long key, long value);

    public abstract long get(long key);

}