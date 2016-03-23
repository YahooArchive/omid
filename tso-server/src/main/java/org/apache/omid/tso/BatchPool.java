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

import javax.inject.Inject;
import java.util.Stack;

class BatchPool {

    final private Batch[] batches;
    final private int poolSize;
    final private Stack<Integer> availableBatches;

    @Inject
    public BatchPool(TSOServerConfig config) {

        int numBuffersPerHandler = (config.getNumBuffersPerHandler() >= 2) ? config.getNumBuffersPerHandler() : 2;
        poolSize = config.getPersistHandlerNum() * numBuffersPerHandler;
        batches = new Batch[poolSize];
        int batchSize = (config.getMaxBatchSize() / config.getPersistHandlerNum() > 0) ? (config.getMaxBatchSize() / config.getPersistHandlerNum()) : 2;

        for (int i = 0; i < poolSize; i++) {
            batches[i] = new Batch(batchSize, i, this);
        }

        availableBatches = new Stack<>();

        for (int i = (poolSize - 1); i >= 0; i--) {
            availableBatches.push(i);
        }

    }

    Batch getNextEmptyBatch() throws InterruptedException {

        synchronized (availableBatches) {
            while (availableBatches.isEmpty()) {
                availableBatches.wait();
            }

            Integer batchIdx = availableBatches.pop();
            return batches[batchIdx];
        }

    }

    void notifyEmptyBatch(int batchIdx) {

        synchronized (availableBatches) {
            availableBatches.push(batchIdx);
            availableBatches.notify();
        }

    }

    public void reset() {

        for (int i = 0; i < poolSize; i++) {
            batches[i].clear();
        }

        availableBatches.clear();

        for (int i = (poolSize - 1); i >= 0; i--) {
            availableBatches.push(i);
        }

    }

}
