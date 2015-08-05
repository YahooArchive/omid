package com.yahoo.omid.tso;

import org.jboss.netty.channel.Channel;

import java.util.Collection;

interface RequestProcessor {

    void timestampRequest(Channel c);

    void commitRequest(long startTimestamp, Collection<Long> writeSet, boolean isRetry, Channel c);

}
