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

import static com.codahale.metrics.MetricRegistry.name;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The Timestamp Oracle that gives monotonically increasing timestamps
 */
public class TimestampOracle {

    private static final Logger LOG = LoggerFactory.getLogger(TimestampOracle.class);

    public interface TimestampStorage {

        public void updateMaxTimestamp(long previousMaxTimestamp, long newMaxTimestamp) throws IOException;

        public long getMaxTimestamp() throws IOException;
    }

    /**
     * Used for testing
     */
    public static class InMemoryTimestampStorage implements TimestampStorage {

        long maxTimestamp = 0;

        @Override
        public void updateMaxTimestamp(long previousMaxTimestamp, long nextMaxTimestamp) {
            maxTimestamp = nextMaxTimestamp;
            LOG.info("Updating max timestamp: (previous:{}, new:{})", previousMaxTimestamp, nextMaxTimestamp);
        }

        @Override
        public long getMaxTimestamp() {
            return maxTimestamp;
        }

    }

    private class AllocateTimestampBatchTask implements Runnable {
        long previousMaxTimestamp;

        public AllocateTimestampBatchTask(long previousMaxTimestamp) {
            this.previousMaxTimestamp = previousMaxTimestamp;
        }
        
        @Override
        public void run() {
            long newMaxTimestamp = previousMaxTimestamp + TIMESTAMP_BATCH;
            try {
                storage.updateMaxTimestamp(previousMaxTimestamp, newMaxTimestamp);
                maxAllocatedTimestamp = newMaxTimestamp;
                previousMaxTimestamp = newMaxTimestamp;
            } catch(IOException e) {
                LOG.error("Can't store the new max timestamp", e);
                // FIXME Should panic process
            }
        }

    }

    static final long TIMESTAMP_BATCH = 10 * 1000 * 1000; // 10 million
    static final long TIMESTAMP_REMAINING_THRESHOLD = 1 * 1000 * 1000; // 1 million

    private long lastTimestamp;

    private long maxTimestamp;

    private TimestampStorage storage;

    private long nextAllocationThreshold;
    private volatile long maxAllocatedTimestamp;

    private Executor executor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("ts-persist-%d").build());

    private final Runnable allocateTimestampsBatchTask;

    /**
     * Returns the next timestamp if available. Otherwise spins till the
     * ts-persist thread performs the new timestamp allocation
     */
    public long next() throws IOException {
        lastTimestamp++;

        if (lastTimestamp == nextAllocationThreshold) {
            executor.execute(allocateTimestampsBatchTask);
        }
        
        if (lastTimestamp >= maxTimestamp) {
            assert(maxTimestamp <= maxAllocatedTimestamp);
            while(maxAllocatedTimestamp == maxTimestamp) {
                // spin
            }
            assert(maxAllocatedTimestamp > maxTimestamp);
            maxTimestamp = maxAllocatedTimestamp;
            nextAllocationThreshold = maxTimestamp - TIMESTAMP_REMAINING_THRESHOLD;
            assert(nextAllocationThreshold > lastTimestamp && nextAllocationThreshold < maxTimestamp);
            assert(lastTimestamp < maxTimestamp);
        }

        return lastTimestamp;
    }

    public long getLast() {
        return lastTimestamp;
    }
    
    /**
     * Constructor
     */
    public TimestampOracle(MetricRegistry metrics, TimestampStorage tsStorage) throws IOException {
        this.storage = tsStorage;
        this.lastTimestamp = this.maxTimestamp = tsStorage.getMaxTimestamp();
        this.allocateTimestampsBatchTask = new AllocateTimestampBatchTask(lastTimestamp);
        
        // Trigger first allocation of timestamps
        executor.execute(allocateTimestampsBatchTask);
        
        metrics.register(name("tso", "maxTimestamp"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return maxTimestamp;
            }
        });
        LOG.info("Initializing timestamp oracle with timestamp {}", this.lastTimestamp);
    }

    @Override
    public String toString() {
        return String.format("TimestampOracle -> LastTimestamp: %d, MaxTimestamp: %d", lastTimestamp, maxTimestamp);
    }
    
}
