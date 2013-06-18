/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Stores uncommitted transactions. First stored transaction = position*bucketSize
 */
public class Bucket {

    private static final Log LOG = LogFactory.getLog(Bucket.class);

    private final long bucketSize;
    private final BitSet transactions;
    private final long position;
    // Pointer to next bucket in case of overlapping in the circular buffer
    Bucket next;

    public Bucket(long position, int bucketSize) {
        this.position = position;
        this.bucketSize = bucketSize;
        this.transactions = new BitSet(bucketSize);
    }

    public boolean isUncommited(long id) {
        return transactions.get((int) (id % bucketSize));
    }

    public Set<Long> abortAllUncommited() {
        Set<Long> result = abortUncommited(bucketSize - 1);
        return result;
    }

    public synchronized Set<Long> abortUncommited(long id) {
        int lastCommited = (int) (id % bucketSize);

        Set<Long> aborted = new HashSet<Long>();

        LOG.trace("Performing scanning...");

        // Clear all set bits (uncommitted transactions) and clear them
        for (int i = transactions.nextSetBit(0); i >= 0 && i <= lastCommited; i = transactions.nextSetBit(i + 1)) {
            aborted.add(position * bucketSize + i);
            transactions.clear(i);
        }

        return aborted;
    }

    public void commit(long transaction) {
        transactions.clear((int) (transaction % bucketSize));
    }

    public void start(long transaction) {
        transactions.set((int) (transaction % bucketSize));
    }

    public long getBucketSize() {
        return bucketSize;
    }

    public long getPosition() {
        return position;
    }

}
