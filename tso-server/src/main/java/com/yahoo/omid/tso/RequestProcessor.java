package com.yahoo.omid.tso;

import org.jboss.netty.channel.Channel;

import java.io.IOException;
import java.util.Collection;

interface RequestProcessor {

    /**
     * Allows to reset the component state
     *
     * @throws IOException
     */
    void resetState() throws IOException;

    /**
     * Exposes the TSO epoch.
     * Required for implementing High Availability.
     *
     * @return the current TSO epoch
     */
    long epoch();

    void timestampRequest(Channel c);

    void commitRequest(long startTimestamp, Collection<Long> writeSet, boolean isRetry, Channel c);

}
