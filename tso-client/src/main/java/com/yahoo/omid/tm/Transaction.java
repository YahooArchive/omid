package com.yahoo.omid.tm;

/**
 * This interface defines the transaction state & behavior exposed to users.
 */
public interface Transaction {
    
    public enum Status {
        RUNNING, COMMITTED, ROLLEDBACK
    }

    /**
     * Returns the transaction identifier
     * @return the transaction identifier
     */
    public long getTransactionId();

    /**
     * Returns the current transaction {@link Status}
     * @return transaction status
     */
    public Status getStatus();

    /**
     * Forces the transaction to rollback, even when there's an intention
     * to commit it.
     */
    public void setRollbackOnly();

    /**
     * Returns whether the transaction was marked for rollback or not
     * @return whether the transaction is marked for rollback or not
     */
    public boolean isRollbackOnly();
    
}
