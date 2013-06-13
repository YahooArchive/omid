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

public class Uncommitted {
    private static final Logger LOG = LoggerFactory.getLogger(TSOHandler.class);

    private final long bucketNumber;
    private final long bucketSize;

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
            bucket = new Bucket(absolutePosition, (int) bucketSize);
            buckets[position] = bucket;
        } else if (bucket.getPosition() != absolutePosition) {
            throw new RuntimeException("Overlapping on circular buffer");
        }
        return bucket;
    }

    public void start(long transaction) {
        getBucketByTimestamp(transaction).start(transaction);
    }

    public void commit(long transaction) {
        getBucketByTimestamp(transaction).commit(transaction);
    }

    public void abort(long transaction) {
        commit(transaction);
    }

    public boolean isUncommitted(long transaction) {
        return getBucketByTimestamp(transaction).isUncommited(transaction);
    }

    public Set<Long> raiseLargestDeletedTransaction(long newLargestDeletedTimestamp) {
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
        buckets[position] = null;
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
}
