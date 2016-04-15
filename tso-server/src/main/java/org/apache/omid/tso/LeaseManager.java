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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.omid.tso.TSOStateManager.TSOState;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Encompasses all the required elements to control the leases required for
 * identifying the master instance when running multiple TSO instances for HA
 * It delegates the initialization of the TSO state and the publication of
 * the instance information when getting the lease to an asynchronous task to
 * continue managing the leases without interruptions.
 */
class LeaseManager extends AbstractScheduledService implements LeaseManagement {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseManager.class);

    private final CuratorFramework zkClient;

    private final Panicker panicker;

    private final String tsoHostAndPort;

    private final TSOStateManager stateManager;
    private final ExecutorService tsoStateInitializer = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                    .setNameFormat("tso-state-initializer")
                    .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        @Override
                        public void uncaughtException(Thread t, Throwable e) {
                            panicker.panic(t + " threw exception", e);
                        }
                    })
                    .build());


    private final long leasePeriodInMs;
    private final TSOChannelHandler tsoChannelHandler;
    private int leaseNodeVersion;
    private final AtomicLong endLeaseInMs = new AtomicLong(0L);
    private final AtomicLong baseTimeInMs = new AtomicLong(0L);

    private final String leasePath;
    private final String currentTSOPath;

    LeaseManager(String tsoHostAndPort,
                 TSOChannelHandler tsoChannelHandler,
                 TSOStateManager stateManager,
                 long leasePeriodInMs,
                 String leasePath,
                 String currentTSOPath,
                 CuratorFramework zkClient,
                 Panicker panicker) {

        this.tsoHostAndPort = tsoHostAndPort;
        this.tsoChannelHandler = tsoChannelHandler;
        this.stateManager = stateManager;
        this.leasePeriodInMs = leasePeriodInMs;
        this.leasePath = leasePath;
        this.currentTSOPath = currentTSOPath;
        this.zkClient = zkClient;
        this.panicker = panicker;
        LOG.info("LeaseManager {} initialized. Lease period {}ms", toString(), leasePeriodInMs);

    }

    // ----------------------------------------------------------------------------------------------------------------
    // LeaseManagement implementation
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    public void startService() throws LeaseManagementException {
        createLeaseManagementZNode();
        createCurrentTSOZNode();
        startAndWait();
    }

    @Override
    public void stopService() throws LeaseManagementException {
        stopAndWait();
    }

    @Override
    public boolean stillInLeasePeriod() {
        return System.currentTimeMillis() <= getEndLeaseInMs();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // End LeaseManagement implementation
    // ----------------------------------------------------------------------------------------------------------------

    void tryToGetInitialLeasePeriod() throws Exception {
        baseTimeInMs.set(System.currentTimeMillis());
        if (canAcquireLease()) {
            endLeaseInMs.set(baseTimeInMs.get() + leasePeriodInMs);
            LOG.info("{} got the lease (Master) Ver. {}/End of lease: {}ms", tsoHostAndPort,
                    leaseNodeVersion, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(endLeaseInMs));
            tsoStateInitializer.submit(new Runnable() {
                // TSO State initialization
                @Override
                public void run() {
                    try {
                        TSOState newTSOState = stateManager.reset();
                        advertiseTSOServerInfoThroughZK(newTSOState.getEpoch());
                        tsoChannelHandler.reconnect();
                    } catch (Exception e) {
                        Thread t = Thread.currentThread();
                        t.getUncaughtExceptionHandler().uncaughtException(t, e);
                    }
                }
            });
        } else {
            tsoStateInitializer.submit(new Runnable() {
                // TSO State initialization
                @Override
                public void run() {
                    // In case the TSO was paused close the connection
                    tsoChannelHandler.closeConnection();
                }
            });
        }
    }

    void tryToRenewLeasePeriod() throws Exception {
        baseTimeInMs.set(System.currentTimeMillis());
        if (canAcquireLease()) {
            if (System.currentTimeMillis() > getEndLeaseInMs()) {
                endLeaseInMs.set(0L);
                LOG.warn("{} expired lease! Releasing lease to start Master re-election", tsoHostAndPort);
                tsoChannelHandler.closeConnection();
            } else {
                endLeaseInMs.set(baseTimeInMs.get() + leasePeriodInMs);
                LOG.trace("{} renewed lease: Version {}/End of lease at {}ms",
                        tsoHostAndPort, leaseNodeVersion, endLeaseInMs);
            }
        } else {
            endLeaseInMs.set(0L);
            LOG.warn("{} lost the lease (Ver. {})! Other instance is now Master",
                    tsoHostAndPort, leaseNodeVersion);
            tsoChannelHandler.closeConnection();
        }
    }

    private boolean haveLease() {
        return stillInLeasePeriod();
    }

    private long getEndLeaseInMs() {
        return endLeaseInMs.get();
    }

    private boolean canAcquireLease() throws Exception {
        try {
            int previousLeaseNodeVersion = leaseNodeVersion;
            final byte[] instanceInfo = tsoHostAndPort.getBytes(Charsets.UTF_8);
            // Try to acquire the lease
            Stat stat = zkClient.setData().withVersion(previousLeaseNodeVersion)
                    .forPath(leasePath, instanceInfo);
            leaseNodeVersion = stat.getVersion();
            LOG.trace("{} got new lease version {}", tsoHostAndPort, leaseNodeVersion);
        } catch (KeeperException.BadVersionException e) {
            return false;
        }
        return true;
    }

    // ----------------------------------------------------------------------------------------------------------------
    // AbstractScheduledService implementation
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    protected void startUp() {
    }

    @Override
    protected void shutDown() {
        try {
            tsoChannelHandler.close();
            LOG.info("Channel handler closed");
        } catch (IOException e) {
            LOG.error("Error closing TSOChannelHandler", e);
        }
    }

    @Override
    protected void runOneIteration() throws Exception {

        if (!haveLease()) {
            tryToGetInitialLeasePeriod();
        } else {
            tryToRenewLeasePeriod();
        }

    }

    @Override
    protected Scheduler scheduler() {

        final long guardLeasePeriodInMs = leasePeriodInMs / 4;

        return new AbstractScheduledService.CustomScheduler() {

            @Override
            protected Schedule getNextSchedule() throws Exception {
                if (!haveLease()) {
                    // Get the current node version...
                    Stat stat = zkClient.checkExists().forPath(leasePath);
                    leaseNodeVersion = stat.getVersion();
                    LOG.trace("{} will try to get lease (with Ver. {}) in {}ms", tsoHostAndPort, leaseNodeVersion,
                            leasePeriodInMs);
                    // ...and wait the lease period
                    return new Schedule(leasePeriodInMs, TimeUnit.MILLISECONDS);
                } else {
                    long waitTimeInMs = getEndLeaseInMs() - System.currentTimeMillis() - guardLeasePeriodInMs;
                    LOG.trace("{} will try to renew lease (with Ver. {}) in {}ms", tsoHostAndPort,
                            leaseNodeVersion, waitTimeInMs);
                    return new Schedule(waitTimeInMs, TimeUnit.MILLISECONDS);
                }
            }
        };

    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper methods
    // ----------------------------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return tsoHostAndPort;
    }

    private void createLeaseManagementZNode() throws LeaseManagementException {
        try {
            validateZKPath(leasePath);
        } catch (Exception e) {
            throw new LeaseManagementException("Error creating Lease Management ZNode", e);
        }
    }

    private void createCurrentTSOZNode() throws LeaseManagementException {
        try {
            validateZKPath(currentTSOPath);
        } catch (Exception e) {
            throw new LeaseManagementException("Error creating TSO ZNode", e);
        }
    }

    private void validateZKPath(String zkPath) throws Exception {
        EnsurePath path = zkClient.newNamespaceAwareEnsurePath(zkPath);
        path.ensure(zkClient.getZookeeperClient());
        Stat stat = zkClient.checkExists().forPath(zkPath);
        Preconditions.checkNotNull(stat);
        LOG.info("Path {} ensured", path.getPath());
    }

    private void advertiseTSOServerInfoThroughZK(long epoch) throws Exception {

        Stat previousTSOZNodeStat = new Stat();
        byte[] previousTSOInfoAsBytes = zkClient.getData().storingStatIn(previousTSOZNodeStat).forPath(currentTSOPath);
        if (previousTSOInfoAsBytes != null && !new String(previousTSOInfoAsBytes, Charsets.UTF_8).isEmpty()) {
            String previousTSOInfo = new String(previousTSOInfoAsBytes, Charsets.UTF_8);
            String[] previousTSOAndEpochArray = previousTSOInfo.split("#");
            Preconditions.checkArgument(previousTSOAndEpochArray.length == 2, "Incorrect TSO Info found: ", previousTSOInfo);
            long oldEpoch = Long.parseLong(previousTSOAndEpochArray[1]);
            if (oldEpoch > epoch) {
                throw new LeaseManagementException("Another TSO replica was found " + previousTSOInfo);
            }
        }
        String tsoInfoAsString = tsoHostAndPort + "#" + Long.toString(epoch);
        byte[] tsoInfoAsBytes = tsoInfoAsString.getBytes(Charsets.UTF_8);
        zkClient.setData().withVersion(previousTSOZNodeStat.getVersion()).forPath(currentTSOPath, tsoInfoAsBytes);
        LOG.info("TSO instance {} (Epoch {}) advertised through ZK", tsoHostAndPort, epoch);

    }

}
