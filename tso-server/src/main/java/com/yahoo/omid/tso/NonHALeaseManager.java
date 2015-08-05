package com.yahoo.omid.tso;

import java.io.IOException;

public class NonHALeaseManager implements LeaseManagement {

    public NonHALeaseManager(TSOStateManager stateManager) throws IOException {
        stateManager.reset();
    }

    @Override
    public void startService() throws LeaseManagementException {
    }

    @Override
    public void stopService() throws LeaseManagementException {
    }

    @Override
    public boolean stillInLeasePeriod() {
        // We should always return true
        return true;
    }

}
