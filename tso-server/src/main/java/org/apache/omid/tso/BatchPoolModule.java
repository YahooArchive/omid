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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

public class BatchPoolModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(BatchPoolModule.class);

    private final TSOServerConfig config;

    public BatchPoolModule(TSOServerConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    ObjectPool<Batch> getBatchPool() throws Exception {

        int poolSize = config.getNumConcurrentCTWriters();
        int batchSize = config.getBatchSizePerCTWriter() + 1; // Add 1 element to batch size for storing LWM

        LOG.info("Pool Size (# of Batches) {}; Batch Size {} (including LWM bucket)", poolSize, batchSize);
        LOG.info("Total Batch Size (Pool size * Batch Size): {}", poolSize * batchSize);
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(poolSize);
        config.setBlockWhenExhausted(true);
        GenericObjectPool<Batch> batchPool = new GenericObjectPool<>(new Batch.BatchFactory(batchSize), config);
        LOG.info("Pre-creating objects in the pool..."); // TODO There should be a better way to do this
        List<Batch> batches = new ArrayList<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            batches.add(batchPool.borrowObject());
        }
        for (Batch batch : batches) {
            batchPool.returnObject(batch);
        }
        return batchPool;

    }

}
