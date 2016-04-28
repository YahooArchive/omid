/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid.tso;

import org.jboss.netty.channel.Channel;

interface ReplyProcessor {

    // TODO This documentation does not corresponds to the method below anymore. Put in the right place or remove
    /**
     * Informs the client about the outcome of the Tx it was trying to commit.
     *
     * @param batch
     *            the batch of operations
     * @param batchID
     *            the id of the batch, used to enforce order between replies
     * @param makeHeuristicDecision
     *            informs about whether heuristic actions are needed or not
     * @param startTimestamp
     *            the start timestamp of the transaction (a.k.a. tx id)
     * @param commitTimestamp
     *            the commit timestamp of the transaction
     * @param channel
     *            the communication channed with the client
     */
    void batchResponse(Batch batch, long batchID);

    void addAbort(Batch batch, long startTimestamp, Channel c, MonitoringContext context);

    void addCommit(Batch batch, long startTimestamp, long commitTimestamp, Channel c, MonitoringContext context);

}

