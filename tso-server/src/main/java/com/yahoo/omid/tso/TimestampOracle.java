package com.yahoo.omid.tso;

import java.io.IOException;

public interface TimestampOracle {

    public void initialize() throws IOException;

    /**
     * Returns the next timestamp if available. Otherwise spins till the
     * ts-persist thread performs the new timestamp allocation
     */
    public long next() throws IOException;

    public long getLast();

}