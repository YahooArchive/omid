package com.yahoo.omid.tso;

import org.jboss.netty.channel.Channel;

interface ReplyProcessor
{
    /**
     * Informs the client about the outcome of the Tx it was trying to
     * commit. If the heuristic decision flat is enabled, the client
     * will need to do additional actions for learning the final outcome.
     *
     * @param makeHeuristicDecision
     *            informs about whether heuristic actions are needed or not
     * @param startTimestamp
     *            the start timestamp of the transaction (a.k.a. tx id)
     * @param commitTimestamp
     *            the commit timestamp of the transaction
     * @param channel
     *            the communication channed with the client
     */
    void commitResponse(boolean makeHeuristicDecision, long startTimestamp, long commitTimestamp, Channel channel, MonitoringContext monCtx);
    void abortResponse(long startTimestamp, Channel c, MonitoringContext monCtx);
    void timestampResponse(long startTimestamp, Channel c, MonitoringContext monCtx);
}

