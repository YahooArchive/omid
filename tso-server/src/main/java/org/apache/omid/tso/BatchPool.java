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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Stack;

public class BatchPool {

    private static final Logger LOG = LoggerFactory.getLogger(BatchPool.class);

    private final Batch[] batches;
    private final int poolSize;
    private final Stack<Integer> availableBatches;

    @Inject
    public BatchPool(TSOServerConfig config) {

        poolSize = config.getNumConcurrentCTWriters();
        int batchSize = config.getBatchSizePerCTWriter() + 1; // Add 1 element to batch size for storing LWM

        LOG.info("Pool Size (Batches) {}; Batch Size {} (including LWM bucket)", poolSize, batchSize);
        LOG.info("Total Batch Size (Pool size * Batch Size): {}", poolSize * batchSize);
        batches = new Batch[poolSize];

        LOG.info("Creating {} Batches with {} elements each", poolSize, batchSize);
        for (int i = 0; i < poolSize; i++) {
            batches[i] = new Batch(batchSize, i, this);
        }

        availableBatches = new Stack<>();

        for (int i = (poolSize - 1); i >= 0; i--) {
            availableBatches.push(i);
        }

    }

    Batch getNextEmptyBatch() throws InterruptedException {

        synchronized (availableBatches) { // TODO Synchronized can be put at the method level
            while (availableBatches.isEmpty()) {
                availableBatches.wait();
            }

            Integer batchIdx = availableBatches.pop();
            return batches[batchIdx];
        }

    }

    void notifyEmptyBatch(int batchIdx) {

        synchronized (availableBatches) { // TODO Synchronized can be put at the method level
            availableBatches.push(batchIdx);
            availableBatches.notify();
        }

    }

}
