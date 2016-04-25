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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;
import org.apache.omid.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class TSOServer extends AbstractIdleService {

    private static final Logger LOG = LoggerFactory.getLogger(TSOServer.class);

    public static final String TSO_HOST_AND_PORT_KEY = "tso.hostandport";

    @Inject
    private TSOStateManager tsoStateManager;
    @Inject
    private RequestProcessor requestProcessor;

    // ----------------------------------------------------------------------------------------------------------------
    // High availability related variables
    // ----------------------------------------------------------------------------------------------------------------

    @Inject
    private LeaseManagement leaseManagement;

    // ----------------------------------------------------------------------------------------------------------------

    static TSOServer getInitializedTsoServer(TSOServerConfig config) throws IOException {
        LOG.info("Configuring TSO Server...");
        Injector injector = Guice.createInjector(buildModuleList(config));
        LOG.info("TSO Server configured. Creating instance...");
        return injector.getInstance(TSOServer.class);
    }

    private static List<Module> buildModuleList(final TSOServerConfig config) throws IOException {

        List<Module> guiceModules = new ArrayList<>();
        guiceModules.add(config.getTimestampStoreModule());
        guiceModules.add(config.getCommitTableStoreModule());
        guiceModules.add(config.getLeaseModule());
        guiceModules.add(new TSOModule(config));

        guiceModules.add(new Module() {
            @Override
            public void configure(Binder binder) {
                LOG.info("\t* Metrics provider module set to {}", config.getMetrics().getClass());
                binder.bind(MetricsRegistry.class).toInstance(config.getMetrics());
            }
        });
        return guiceModules;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // AbstractIdleService implementation
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    protected void startUp() throws Exception {
        tsoStateManager.register(requestProcessor);
        leaseManagement.startService();
        LOG.info("********** TSO Server running **********");
    }

    @Override
    protected void shutDown() throws Exception {
        leaseManagement.stopService();
        tsoStateManager.unregister(requestProcessor);
        LOG.info("********** TSO Server stopped successfully **********");
    }

    // ----------------------------------------------------------------------------------------------------------------

    private void attachShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                stopAndWait();
            }
        });
        LOG.info("Shutdown Hook Attached");
    }

    /**
     * This is where all starts on the server side
     */
    public static void main(String[] args) {

        TSOServerConfig config = new TSOServerConfig();

        try {
            TSOServer tsoServer = getInitializedTsoServer(config);
            tsoServer.attachShutDownHook();
            tsoServer.startAndWait();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }

    }

}
