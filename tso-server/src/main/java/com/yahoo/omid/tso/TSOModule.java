/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.tso;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.yahoo.omid.timestamp.storage.TimestampStorage;

import javax.inject.Singleton;

import static com.yahoo.omid.committable.hbase.CommitTableConstants.COMMIT_TABLE_NAME_KEY;

class TSOModule extends AbstractModule {

    private final TSOServerCommandLineConfig config;

    TSOModule(TSOServerCommandLineConfig config) {
        this.config = config;
    }

    @Override
    protected void configure() {

        bind(TSOChannelHandler.class).in(Singleton.class);

        bind(TSOStateManager.class).to(TSOStateManagerImpl.class).in(Singleton.class);

        bind(TimestampOracle.class).to(TimestampOracleImpl.class).in(Singleton.class);
        bind(Panicker.class).to(SystemExitPanicker.class).in(Singleton.class);

        bindConstant().annotatedWith(Names.named(COMMIT_TABLE_NAME_KEY))
            .to(config.getCommitTable());

        bindConstant().annotatedWith(Names.named(TimestampStorage.TIMESTAMPSTORAGE_TABLE_NAME_KEY))
            .to(config.getTimestampTable());

        // Disruptor setup
        install(new DisruptorModule());

        // LeaseManagement setup
        install(new LeaseManagementModule(config));

        // ZK Module
        install(new ZKModule(config));
    }

    @Provides
    TSOServerCommandLineConfig provideTSOServerConfig() {
        return config;
    }

}
