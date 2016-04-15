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
package org.apache.omid.transaction;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.omid.timestamp.storage.ZKModule;
import org.apache.omid.tso.LeaseManagement;
import org.apache.omid.tso.Panicker;
import org.apache.omid.tso.PausableLeaseManager;
import org.apache.omid.tso.TSOChannelHandler;
import org.apache.omid.tso.TSOStateManager;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;

import static org.apache.omid.tso.TSOServer.TSO_HOST_AND_PORT_KEY;

class TestHALeaseManagementModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(TestHALeaseManagementModule.class);
    private final long leasePeriodInMs;
    private final String tsoLeasePath;
    private final String currentTsoPath;
    private final String zkCluster;
    private final String zkNamespace;

    TestHALeaseManagementModule(long leasePeriodInMs, String tsoLeasePath, String currentTsoPath,
                                String zkCluster, String zkNamespace) {
        this.leasePeriodInMs = leasePeriodInMs;
        this.tsoLeasePath = tsoLeasePath;
        this.currentTsoPath = currentTsoPath;
        this.zkCluster = zkCluster;
        this.zkNamespace = zkNamespace;
    }

    @Override
    protected void configure() {
        install(new ZKModule(zkCluster, zkNamespace));
    }

    @Provides
    @Singleton
    LeaseManagement provideLeaseManager(@Named(TSO_HOST_AND_PORT_KEY) String tsoHostAndPort,
                                        TSOChannelHandler tsoChannelHandler,
                                        TSOStateManager stateManager,
                                        CuratorFramework zkClient,
                                        Panicker panicker)
            throws LeaseManagement.LeaseManagementException {

        LOG.info("Connection to ZK cluster [{}]", zkClient.getState());
        return new PausableLeaseManager(tsoHostAndPort, tsoChannelHandler, stateManager, leasePeriodInMs,
                                        tsoLeasePath, currentTsoPath, zkClient, panicker);

    }

}
