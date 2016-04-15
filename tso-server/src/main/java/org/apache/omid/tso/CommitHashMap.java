/**
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores the mapping between a particular cell id and the commit timestamp
 * of the last transaction that changed it.
 *
 * The mapping is implemented as a long -> long mapping, using a single long [].
 * For a map of size N we create an array of size 2*N and store the keys
 * on even indexes and values on odd indexes. The rationale is that we want
 * queries to be fast and touch as least memory regions as possible.
 *
 * Each time an entry is removed, the caller updates the largestDeletedTimestamp
 * if the entry's commit timestamp is greater than this value.
 *
 * TODO: improve garbage collection, right now an entry is picked at random
 * (by hash) which could cause the eviction of a very recent timestamp
 */

class CommitHashMap {

    private static final Logger LOG = LoggerFactory.getLogger(CommitHashMap.class);

    private final Cache cellIdToCommitMap;

    /**
     * Constructs a new, empty hashtable with a default size of 1000
     */
    public CommitHashMap() {
        this(1000);
    }

    /**
     * Constructs a new, empty hashtable with the specified size
     *
     * @param size
     *            the initial size of the hashtable.
     * @throws IllegalArgumentException
     *             if the size is less than zero.
     */
    public CommitHashMap(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Illegal size: " + size);
        }

        this.cellIdToCommitMap = new LongCache(size, 32);
        LOG.info("CellId -> CommitTS map created with [{}] buckets (32 elems/bucket)", size);
    }

    public void reset() {
        cellIdToCommitMap.reset();
        LOG.info("CellId -> CommitTS map reset");
    }

    public long getLatestWriteForCell(long hash) {
        return cellIdToCommitMap.get(hash);
    }

    public long putLatestWriteForCell(long hash, long commitTimestamp) {
        return cellIdToCommitMap.set(hash, commitTimestamp);
    }
}
