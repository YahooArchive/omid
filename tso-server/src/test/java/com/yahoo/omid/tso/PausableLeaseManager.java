package com.yahoo.omid.tso;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.tso.TSOServer.LeaseManager;

public class PausableLeaseManager extends LeaseManager {

    private static final Logger LOG = LoggerFactory.getLogger(PausableLeaseManager.class);

    private volatile boolean pausedInTryToGetInitialLeasePeriod = false;
    private volatile boolean pausedInTryToRenewLeasePeriod = false;
    private volatile boolean pausedInStillInLeasePeriod = false;

    public PausableLeaseManager(String id,
                                long epoch,
                                long leasePeriodInMs,
                                String tsoLeasePath,
                                String currentTSOPath,
                                CuratorFramework zkClient) {
        super(id, epoch, leasePeriodInMs, tsoLeasePath, currentTSOPath, zkClient);
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

    // **************** Helper Methods to pause functionality *****************

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