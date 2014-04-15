package com.yahoo.omid.tso;

import org.jboss.netty.channel.Channel;

interface ReplyProcessor 
{
    void commitResponse(long startTimestamp, long commitTimestamp, Channel c);
    void abortResponse(long startTimestamp, Channel c);
    void timestampResponse(long startTimestamp, Channel c);
}

