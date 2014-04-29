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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

/**
 * The Timestamp Oracle that gives monotonically increasing timestamps
 * 
 * @author maysam
 * 
 */

public class TimestampOracle {

    private static final Logger LOG = LoggerFactory.getLogger(TimestampOracle.class);

    public interface TimestampStorage {

        public void updateMaxTimestamp(long previousMaxTimestamp, long newMaxTimestamp) throws IOException;

        public long getMaxTimestamp() throws IOException;
    }

    public static class InMemoryTimestampStorage implements TimestampStorage {

        long maxTimestamp = 0;

        @Override
        public void updateMaxTimestamp(long previousMaxTimestamp, long nextMaxTimestamp) {
            maxTimestamp = nextMaxTimestamp;
        }

        @Override
        public long getMaxTimestamp() {
            return maxTimestamp;
        }

    }

    private static final long TIMESTAMP_BATCH = 100000;

    private long lastTimestamp;

    private long maxTimestamp;

    private TimestampStorage tsStorage;    

    /**
     * Must be called holding an exclusive lock
     * 
     * return the next timestamp
     */
    public long next() throws IOException {
        lastTimestamp++;
        if (lastTimestamp == maxTimestamp) {
            long previousMaxTimestamp = maxTimestamp;
            maxTimestamp += TIMESTAMP_BATCH;
            tsStorage.updateMaxTimestamp(previousMaxTimestamp, maxTimestamp);
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
        this.tsStorage = tsStorage;
        this.lastTimestamp = this.maxTimestamp = tsStorage.getMaxTimestamp();
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
