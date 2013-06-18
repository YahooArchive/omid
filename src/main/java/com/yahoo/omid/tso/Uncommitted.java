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

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores uncommitted transactions, which have to be aborted when the LowWatermark (largestDeletedTimestamp) is 
 * increased. Once committed or aborted, the transactions must be marked as not-uncommitted.
 */
public class Uncommitted {
    private static final Logger LOG = LoggerFactory.getLogger(TSOHandler.class);

    private final long bucketNumber;
    private final long bucketSize;

    /* 
     *  Circular buffer that stores the actual uncommitted transactions. Each bucket is actually a BitSet
     * of size 'bucketSize'. Uncommitted transactions are represented as 'set bits' (1s) and committed, aborted or not
     * yet started transactions are 'clear bits' (0s).
     * 
     *  Each bucket has a 'next' pointer which is used in case the circular buffer overlaps, which should happen
     *  infrequently.
     *
     *  Example:
     *  
     *  [4] -> { absPos = 14 }
     *  [3] -> { absPos = 13 }
     *  [2] -> { absPos = 12 }
     *  [1] -> { absPos = 11 } -> { absPos = 16 }
     *  [0] -> { absPos = 10 } -> { absPos = 15 }
     *  
     *  Buckets for absolutePosition 10 and 15 are both stored at index 0 in the circular buffer. After the 
     *  largestDeletedTimestamp is increased:
     *  
     *  [4] -> { absPos = 14 }
     *  [3] -> null
     *  [2] -> null
     *  [1] -> { absPos = 16 }
     *  [0] -> { absPos = 15 }
     */
    private Bucket buckets[];
    private long largestDeletedTimestamp;

    public Uncommitted(long startTimestamp, int bucketNumber, int bucketSize) {
        this.bucketSize = bucketSize;
        this.bucketNumber = bucketNumber;
        this.buckets = new Bucket[(int) bucketNumber];
        this.largestDeletedTimestamp = startTimestamp;
        LOG.debug("Start TS : " + startTimestamp);
        LOG.debug("BKT_NUMBER : " + bucketNumber + " BKT_SIZE: " + bucketSize);
    }

    private Bucket getBucketByTimestamp(long transaction) {
        int position = getRelativePosition(transaction);
        long absolutePosition = getAbsolutePosition(transaction);
        return getBucketByRelativePosition(position, absolutePosition);
    }

    private Bucket getBucketByAbsolutePosition(long absolutePosition) {
        int position = (int) (absolutePosition % bucketNumber);
        return getBucketByRelativePosition(position, absolutePosition);
    }

    private Bucket getBucketByRelativePosition(int position, long absolutePosition) {
        Bucket bucket = buckets[position];
        if (bucket == null) {
            // If there is no bucket, create and return it
            bucket = new Bucket(absolutePosition, (int) bucketSize);
            buckets[position] = bucket;
            return bucket;
        }
        // Look for the correct bucket, ther might have been an overlap in the circular buffer
        while (bucket.getPosition() != absolutePosition && bucket.next != null) {
            if ((bucket.getPosition() + 1) * bucketSize < largestDeletedTimestamp) {
                throw new RuntimeException("Old bucket wasn't collected on LW increase");
            }
            bucket = bucket.next;
        }
        if (bucket.getPosition() == absolutePosition) {
            // If there is an existing bucket for this transaction, return it
            return bucket;
        }
        // Otherwise there is an overlap (bucket exists for this relative position, but not for this absolute position)
        // Create new bucket and asign it to the next pointer of the previous one.
        bucket.next = new Bucket(absolutePosition, (int) bucketSize);;
        return bucket.next;
    }

    public void start(long transaction) {
        if (transaction <= largestDeletedTimestamp) {
            throw new IllegalArgumentException("Timestamp " + transaction + " is older than largestDeletedTimestamp "
                    + largestDeletedTimestamp);
        }
        getBucketByTimestamp(transaction).start(transaction);
    }

    public void commit(long transaction) {
        if (transaction <= largestDeletedTimestamp) {
            throw new IllegalArgumentException("Timestamp " + transaction + " is older than largestDeletedTimestamp "
                    + largestDeletedTimestamp);
        }
        getBucketByTimestamp(transaction).commit(transaction);
    }

    public void abort(long transaction) {
        commit(transaction);
    }

    public boolean isUncommitted(long transaction) {
        return getBucketByTimestamp(transaction).isUncommited(transaction);
    }

    public Set<Long> raiseLargestDeletedTransaction(long newLargestDeletedTimestamp) {
        if (newLargestDeletedTimestamp <= largestDeletedTimestamp) {
            throw new IllegalArgumentException("Received older largestDeletedTimestamp, " + newLargestDeletedTimestamp + " vs "
                    + largestDeletedTimestamp);
        }
        // Last bucket to consider
        Bucket lastBucket = getBucketByTimestamp(newLargestDeletedTimestamp);
        // Position of first bucket to consider
        long currentAbsolutePosition = getAbsolutePosition(largestDeletedTimestamp);
        Bucket bucket;
        Set<Long> aborted = new HashSet<Long>();
        // All but the last bucket are reset
        while ((bucket = getBucketByAbsolutePosition(currentAbsolutePosition)) != lastBucket) {
            aborted.addAll(bucket.abortAllUncommited());
            resetBucket(currentAbsolutePosition);
            currentAbsolutePosition++;
        }
        // Last bucket is only processed partially
        aborted.addAll(bucket.abortUncommited(newLargestDeletedTimestamp));

        largestDeletedTimestamp = newLargestDeletedTimestamp;
        return aborted;
    }

    private void resetBucket(long absolutePosition) {
        int position = (int) (absolutePosition % bucketNumber);
        buckets[position] = buckets[position].next;
    }

    private int getRelativePosition(long id) {
        return (int) ((id / bucketSize) % bucketNumber);
    }

    private long getAbsolutePosition(long id) {
        return id / bucketSize;
    }

    public int getBucketSize() {
        return (int) bucketSize;
    }

    public int getBucketNumber() {
        return (int) bucketNumber;
    }
}
