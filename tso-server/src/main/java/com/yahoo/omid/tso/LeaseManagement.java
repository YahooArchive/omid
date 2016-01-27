package com.yahoo.omid.tso;

public interface LeaseManagement {

    class LeaseManagementException extends Exception {

        private static final long serialVersionUID = -2061376444591776881L;

        LeaseManagementException(String msg) {
            super(msg);
        }


        LeaseManagementException(String msg, Exception e) {
            super(msg, e);
        }

    }

    /**
     * Allows to start the service implementing the lease management
     */
    void startService() throws LeaseManagementException;

    /**
     * Allows to stop the service implementing the lease management
     */
    void stopService() throws LeaseManagementException;

    /**
     * Check if the instance is still is under the lease period
     */
    boolean stillInLeasePeriod();

}
