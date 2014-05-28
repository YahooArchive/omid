package com.yahoo.omid.tm;

public class TransactionManagerException extends Exception {

    private static final long serialVersionUID = 6811947817962260774L;

    public TransactionManagerException(String reason) {
        super(reason);
    }

    public TransactionManagerException(String reason, Throwable e) {
        super(reason, e);
    }
    
}
