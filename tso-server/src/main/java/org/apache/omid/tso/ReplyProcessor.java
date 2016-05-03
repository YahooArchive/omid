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

    /**
     * The each reply to a transactional operation for a client is contained in a batch. The batch must be ordered
     * before sending the replies in order to not to break snapshot isolation properties.
     *
     * @param batchSequence
     *            a batch sequence number, used to enforce order between replies
     * @param batch
     *            a batch containing the transaction operations
     */
    void manageResponsesBatch(long batchSequence, Batch batch);

    // TODO This method can be removed if we return the responses from the retry processor
    void addAbort(Batch batch, long startTimestamp, Channel c, MonitoringContext context);
    // TODO This method can be removed if we return the responses from the retry processor
    void addCommit(Batch batch, long startTimestamp, long commitTimestamp, Channel c, MonitoringContext context);

}

