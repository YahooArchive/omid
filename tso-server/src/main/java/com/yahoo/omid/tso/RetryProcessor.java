package com.yahoo.omid.tso;

import org.jboss.netty.channel.Channel;

interface RetryProcessor {
    void disambiguateRetryRequestHeuristically(long startTimestamp, Channel c);
}
