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

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PausableLeaseManager extends LeaseManager {

    private static final Logger LOG = LoggerFactory.getLogger(PausableLeaseManager.class);

    private volatile boolean pausedInTryToGetInitialLeasePeriod = false;
    private volatile boolean pausedInTryToRenewLeasePeriod = false;
    private volatile boolean pausedInStillInLeasePeriod = false;

    public PausableLeaseManager(String id,
                                TSOChannelHandler tsoChannelHandler,
                                TSOStateManager stateManager,
                                long leasePeriodInMs,
                                String tsoLeasePath,
                                String currentTSOPath,
                                CuratorFramework zkClient,
                                Panicker panicker) {
        super(id, tsoChannelHandler, stateManager, leasePeriodInMs, tsoLeasePath, currentTSOPath, zkClient, panicker);
    }

    @Override
    public void tryToGetInitialLeasePeriod() throws Exception {
        while (pausedInTryToGetInitialLeasePeriod) {
            synchronized (this) {
                try {
                    LOG.info("{} paused in tryToGetInitialLeasePeriod()", this);
                    this.wait();
                } catch (InterruptedException e) {
                    LOG.error("Interrupted whilst paused");
                    Thread.currentThread().interrupt();
                }
            }
        }
        super.tryToGetInitialLeasePeriod();
    }

    @Override
    public void tryToRenewLeasePeriod() throws Exception {
        while (pausedInTryToRenewLeasePeriod) {
            synchronized (this) {
                try {
                    LOG.info("{} paused in tryToRenewLeasePeriod()", this);
                    this.wait();
                } catch (InterruptedException e) {
                    LOG.error("Interrupted whilst paused");
                    Thread.currentThread().interrupt();
                }
            }
        }
        super.tryToRenewLeasePeriod();
    }

    @Override
    public boolean stillInLeasePeriod() {
        while (pausedInStillInLeasePeriod) {
            synchronized (this) {
                try {
                    LOG.info("{} paused in stillInLeasePeriod()", this);
                    this.wait();
                } catch (InterruptedException e) {
                    LOG.error("Interrupted whilst paused");
                    Thread.currentThread().interrupt();
                }
            }
        }
        return super.stillInLeasePeriod();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper Methods to pause functionality
    // ----------------------------------------------------------------------------------------------------------------

    public synchronized void pausedInTryToGetInitialLeasePeriod() {
        pausedInTryToGetInitialLeasePeriod = true;
        this.notifyAll();
    }

    public synchronized void pausedInTryToRenewLeasePeriod() {
        pausedInTryToRenewLeasePeriod = true;
        this.notifyAll();
    }

    public synchronized void pausedInStillInLeasePeriod() {
        pausedInStillInLeasePeriod = true;
        this.notifyAll();
    }

    public synchronized void resume() {
        pausedInTryToGetInitialLeasePeriod = false;
        pausedInTryToRenewLeasePeriod = false;
        pausedInStillInLeasePeriod = false;
        this.notifyAll();
    }

}