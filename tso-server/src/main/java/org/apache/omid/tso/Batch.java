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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.omid.tso.PersistEvent.Type;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Batch {

    private static final Logger LOG = LoggerFactory.getLogger(Batch.class);

    private final int id;
    private final int size;
    private int numEvents;
    private final PersistEvent[] events;
    private final BatchPool batchPool;

    Batch(int size, int id, BatchPool batchPool) {

        Preconditions.checkArgument(size > 0, "Size must be positive");
        this.size = size;
        this.batchPool = batchPool;
        this.id = id;
        this.numEvents = 0;
        this.events = new PersistEvent[size];
        for (int i = 0; i < size; i++) {
            this.events[i] = new PersistEvent();
        }
        LOG.info("Batch id {} created with size {}", id, size);

    }

    boolean isFull() {

        Preconditions.checkState(numEvents <= size, "numEvents > size");
        return numEvents == size;

    }

    boolean isEmpty() {

        return numEvents == 0;

    }

    boolean isLastEntryEmpty() {

        Preconditions.checkState(numEvents <= size, "numEvents > size");
        return numEvents == (size - 1);

    }

    int getNumEvents() {
        return numEvents;
    }

    PersistEvent getEvent(int i) {

        assert (0 <= i && i < numEvents);
        return events[i];

    }

    void clear() {

        numEvents = 0;
        if (batchPool != null) {
            batchPool.notifyEmptyBatch(id);
        }

    }

    void addCommit(long startTimestamp, long commitTimestamp, Channel c, MonitoringContext context) {
        Preconditions.checkState(!isFull(), "batch is full");
        int index = numEvents++;
        PersistEvent e = events[index];
        e.makePersistCommit(startTimestamp, commitTimestamp, c, context);

    }

    void addAbort(long startTimestamp, boolean isRetry, Channel c, MonitoringContext context) {
        Preconditions.checkState(!isFull(), "batch is full");
        int index = numEvents++;
        PersistEvent e = events[index];
        e.makePersistAbort(startTimestamp, isRetry, c, context);

    }

    void addTimestamp(long startTimestamp, Channel c, MonitoringContext context) {
        Preconditions.checkState(!isFull(), "batch is full");
        int index = numEvents++;
        PersistEvent e = events[index];
        e.makePersistTimestamp(startTimestamp, c, context);

    }

    void addLowWatermark(long lowWatermark, MonitoringContext context) {
        Preconditions.checkState(!isFull(), "batch is full");
        int index = numEvents++;
        PersistEvent e = events[index];
        e.makePersistLowWatermark(lowWatermark, context);

    }

    void sendReply(ReplyProcessor reply, RetryProcessor retryProc, long batchID) {

        int i = 0;
        while (i < numEvents) {
            PersistEvent e = events[i];
            if (e.getType() == Type.ABORT && e.isRetry()) {
                retryProc.disambiguateRetryRequestHeuristically(e.getStartTimestamp(), e.getChannel(), e.getMonCtx());
                PersistEvent tmp = events[i];
                //TODO: why assign it?
                events[i] = events[numEvents - 1];
                events[numEvents - 1] = tmp;
                if (numEvents == 1) {
                    clear();
                    reply.manageResponsesBatch(batchID, null);
                    return;
                }
                numEvents--;
                continue;
            }
            i++;
        }

        reply.manageResponsesBatch(batchID, this);

    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", id)
                .add("size", size)
                .add("num events", numEvents)
                .add("events", Arrays.toString(events))
                .toString();
    }

}
