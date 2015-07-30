package com.yahoo.omid.tso;

public interface LeaseManagement {

    public static class LeaseManagementException extends Exception {

        private static final long serialVersionUID = -2061376444591776881L;

        public LeaseManagementException(String msg) {
            super(msg);
        }

        public LeaseManagementException(String msg, Exception e) {
            super(msg, e);
        }

    }

    /**
     * Allows to start the service implementing the lease management
     */
    public void startService() throws LeaseManagementException;

    /**
     * Allows to stop the service implementing the lease management
     */
    public void stopService() throws LeaseManagementException;

    /**
     * Check if the instance is still is under the lease period
     */
    public boolean stillInLeasePeriod();

}
