package com.yahoo.omid.tso;

import org.jboss.netty.channel.Channel;

interface PersistenceProcessor {
    void persistCommit(long startTimestamp, long commitTimestamp, Channel c);
    void persistAbort(long startTimestamp, boolean isRetry, Channel c);
    void persistTimestamp(long startTimestamp, Channel c);
}
