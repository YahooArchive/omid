package com.yahoo.omid.tso;

import org.jboss.netty.channel.Channel;

interface PersistenceProcessor {
    void persistCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext monCtx);
    void persistAbort(long startTimestamp, boolean isRetry, Channel c, MonitoringContext monCtx);
    void persistTimestamp(long startTimestamp, Channel c, MonitoringContext monCtx);
    void persistLowWatermark(long lowWatermark);
}
