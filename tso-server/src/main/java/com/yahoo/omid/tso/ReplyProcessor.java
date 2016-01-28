/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

