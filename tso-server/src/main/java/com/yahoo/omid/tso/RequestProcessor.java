package com.yahoo.omid.tso;

import org.jboss.netty.channel.Channel;

import java.util.Collection;

// NOTE: public is required explicitly in the interface definition for Guice injection
public interface RequestProcessor extends TSOStateManager.StateObserver {

    public void timestampRequest(Channel c);

    public void commitRequest(long startTimestamp, Collection<Long> writeSet, boolean isRetry, Channel c);

}
