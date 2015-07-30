package com.yahoo.omid.tso;

import static com.yahoo.omid.ZKConstants.CURRENT_TSO_PATH;
import static com.yahoo.omid.ZKConstants.TSO_LEASE_PATH;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService;

/**
 * Encompasses all the required elements to control the leases required for
 * identifying the master instance when running multiple TSO instances for HA
 * This includes publishing the instance information when getting the lease.
 */
public class LeaseManager extends AbstractScheduledService implements LeaseManagement {

    private static final Logger LOG = LoggerFactory.getLogger(LeaseManager.class);

    private final CuratorFramework zkClient;

    private final String tsoHostAndPort;

    private final RequestProcessor requestProcessor;

    private final long leasePeriodInMs;
    private int leaseNodeVersion;
    private final AtomicLong endLeaseInMs = new AtomicLong(0L);
    private final AtomicLong baseTimeInMs = new AtomicLong(0L);

    private final String leasePath;
    private final String currentTSOPath;

    public LeaseManager(String tsoHostAndPort,
                            RequestProcessor requestProcessor,
                            long leasePeriodInMs,
                            CuratorFramework zkClient) {
        this(tsoHostAndPort, requestProcessor, leasePeriodInMs, TSO_LEASE_PATH, CURRENT_TSO_PATH, zkClient);
    }

    @VisibleForTesting
    LeaseManager(String tsoHostAndPort,
                 RequestProcessor requestProcessor,
                 long leasePeriodInMs,
                 String leasePath,
                 String currentTSOPath,
                 CuratorFramework zkClient) {
        this.tsoHostAndPort = tsoHostAndPort;
        this.requestProcessor = requestProcessor;
        this.leasePeriodInMs = leasePeriodInMs;
        this.leasePath = leasePath;
        this.currentTSOPath = currentTSOPath;
        this.zkClient = zkClient;
        LOG.info("LeaseManager {} initialized. Lease period {}ms", toString(), leasePeriodInMs);
    }

    // ------------------------------------------------------------------------
    // ----------------- LeaseManagement implementation -----------------------
    // ------------------------------------------------------------------------

    @Override
    public void startService() throws LeaseManagementException {
        createLeaseManagementZNode();
        createCurrentTSOZNode();
        startAndWait();
    }

    @Override
    public void stopService() throws LeaseManagementException {
        stop();
    }

    @Override
    public boolean stillInLeasePeriod() {
        return System.currentTimeMillis() <= getEndLeaseInMs();
    }

    // ------------------------------------------------------------------------
    // ------------------ End LeaseManagement implementation ------------------
    // ------------------------------------------------------------------------

    void tryToGetInitialLeasePeriod() throws Exception {
        baseTimeInMs.set(System.currentTimeMillis());
        if (canAcquireLease()) {
            endLeaseInMs.set(baseTimeInMs.get() + leasePeriodInMs);
            LOG.info("{} got the lease (Master) Ver. {}/End of lease: {}ms", tsoHostAndPort,
                    leaseNodeVersion, endLeaseInMs);
            advertiseTSOServerInfoThroughZK(requestProcessor.epoch());
        }
    }

    void tryToRenewLeasePeriod() throws Exception {
        baseTimeInMs.set(System.currentTimeMillis());
        if (canAcquireLease()) {
            if (System.currentTimeMillis() > getEndLeaseInMs()) {
                LOG.warn("{} expired lease! Releasing lease to start Master re-election", tsoHostAndPort);
                endLeaseInMs.set(0L);
            } else {
                endLeaseInMs.set(baseTimeInMs.get() + leasePeriodInMs);
                LOG.trace("{} renewed lease: Version {}/End of lease at {}ms",
                        tsoHostAndPort, leaseNodeVersion, endLeaseInMs);
            }
        } else {
            endLeaseInMs.set(0L);
            LOG.warn("{} lost the lease (Ver. {})! Other instance is now Master",
                    tsoHostAndPort, leaseNodeVersion);
        }
    }

    boolean haveLease() {
        return stillInLeasePeriod();
    }

    public long getEndLeaseInMs() {
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

    // ------------------------------------------------------------------------
    // --------------- AbstractScheduledService implementation ----------------
    // ------------------------------------------------------------------------

    @Override
    protected void startUp() {
    }

    @Override
    protected void shutDown() {
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

    // ************************* Helper methods *******************************

    @Override
    public String toString() {
        return tsoHostAndPort;
    }

    void createLeaseManagementZNode() throws LeaseManagementException {

        try {
            EnsurePath path = zkClient.newNamespaceAwareEnsurePath(leasePath);
            path.ensure(zkClient.getZookeeperClient());
            Stat stat = zkClient.checkExists().forPath(leasePath);
            Preconditions.checkNotNull(stat);
            LOG.info("Path {} ensured", path.getPath());
        } catch (Exception e) {
            throw new LeaseManagementException("Error creating Lease Management ZNode", e);
        }
    }

    void createCurrentTSOZNode() throws LeaseManagementException {

        try {
            EnsurePath path = zkClient.newNamespaceAwareEnsurePath(currentTSOPath);
            path.ensure(zkClient.getZookeeperClient());
            Stat stat = zkClient.checkExists().forPath(currentTSOPath);
            Preconditions.checkNotNull(stat);
            LOG.info("Path {} ensured", path.getPath());
        } catch (Exception e) {
            throw new LeaseManagementException("Error creating TSO ZNode", e);
        }

    }

    int advertiseTSOServerInfoThroughZK(long epoch) throws Exception {

        LOG.info("Advertising TSO host:port {} (Epoch {}) through ZK", tsoHostAndPort, epoch);
        String tsoInfoAsString = tsoHostAndPort + "#" + Long.toString(epoch);
        byte[] tsoInfoAsBytes = tsoInfoAsString.getBytes(Charsets.UTF_8);
        Stat currentTSOZNodeStat = zkClient.setData().forPath(currentTSOPath, tsoInfoAsBytes);
        return currentTSOZNodeStat.getVersion();

    }

}
