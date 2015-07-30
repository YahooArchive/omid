package com.yahoo.omid.tso;

public class NonHALeaseManager implements LeaseManagement {

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
