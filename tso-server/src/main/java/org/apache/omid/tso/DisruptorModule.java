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
import com.google.inject.name.Names;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;

import javax.inject.Singleton;

public class DisruptorModule extends AbstractModule {

    private final TSOServerConfig config;

    public DisruptorModule(TSOServerConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {
        switch (config.getWaitStrategyEnum()) {
        // A low-cpu usage Disruptor configuration for using in local/test environments
        case LOW_CPU:
             bind(WaitStrategy.class).annotatedWith(Names.named("PersistenceStrategy")).to(BlockingWaitStrategy.class);
             bind(WaitStrategy.class).annotatedWith(Names.named("ReplyStrategy")).to(BlockingWaitStrategy.class);
             bind(WaitStrategy.class).annotatedWith(Names.named("RetryStrategy")).to(BlockingWaitStrategy.class);
             break;
        // The default high-cpu usage Disruptor configuration for getting high throughput on production environments
        case HIGH_THROUGHPUT:
        default:
             bind(WaitStrategy.class).annotatedWith(Names.named("PersistenceStrategy")).to(BusySpinWaitStrategy.class);
             bind(WaitStrategy.class).annotatedWith(Names.named("ReplyStrategy")).to(BusySpinWaitStrategy.class);
             bind(WaitStrategy.class).annotatedWith(Names.named("RetryStrategy")).to(YieldingWaitStrategy.class);
             break;
        }
        bind(RequestProcessor.class).to(RequestProcessorImpl.class).in(Singleton.class);
        bind(PersistenceProcessor.class).to(PersistenceProcessorImpl.class).in(Singleton.class);
        bind(ReplyProcessor.class).to(ReplyProcessorImpl.class).in(Singleton.class);
        bind(RetryProcessor.class).to(RetryProcessorImpl.class).in(Singleton.class);

    }

}
