package com.yahoo.omid.tso;

import java.io.IOException;

public class NonHALeaseManager implements LeaseManagement {

    private final TSOChannelHandler tsoChannelHandler;
    private TSOStateManager stateManager;

    public NonHALeaseManager(TSOChannelHandler tsoChannelHandler, TSOStateManager stateManager) {
        this.tsoChannelHandler = tsoChannelHandler;
        this.stateManager = stateManager;
    }

    @Override
    public void startService() throws LeaseManagementException {
        try {
            stateManager.reset();
            tsoChannelHandler.reconnect();
        } catch (IOException e) {
            throw new LeaseManagementException("Error initializing Lease Manager", e);
        }
    }

    @Override
    public void stopService() throws LeaseManagementException {
        tsoChannelHandler.closeConnection();
    }

    @Override
    public boolean stillInLeasePeriod() {
        // We should always return true
        return true;
    }

}
