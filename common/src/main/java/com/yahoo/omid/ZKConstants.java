package com.yahoo.omid;

public class ZKConstants {

    public static final String OMID_NAMESPACE = "omid";

    public static final String CURRENT_TSO_PATH = "/current-tso";
    public static final String TSO_LEASE_PATH = "/tso-lease";

    // Avoid instantiation
    private ZKConstants() {
    }

}
