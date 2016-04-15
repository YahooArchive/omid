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

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps TSO Server as a Daemon
 */
public class TsoServerDaemon implements Daemon {

    private static final Logger LOG = LoggerFactory.getLogger(TsoServerDaemon.class);
    private TSOServer tsoServer;

    @Override
    public void init(DaemonContext daemonContext) throws Exception {
        try {
            TSOServerConfig config = new TSOServerConfig();
            LOG.info("Creating TSOServer instance as a Daemon process...");
            tsoServer = TSOServer.getInitializedTsoServer(config);
            LOG.info("TSOServer instance for Daemon process created");
        } catch (Exception e) {
            LOG.error("Error creating TSOServer instance", e);
            throw e;
        }
    }

    @Override
    public void start() throws Exception {
        tsoServer.startAndWait();
    }

    @Override
    public void stop() throws Exception {
        tsoServer.stopAndWait();
    }

    @Override
    public void destroy() {
    }

}
