package com.yahoo.omid.transaction;

import com.google.common.base.Optional;

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
     * Returns the epoch given by the TSOServer
     * @return the transaction's TSOServer epoch
     */
    public long getEpoch();

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



    /**
     * Set of methods to attach some metadata to a transaction object. One example
     * of such metadata are notifications
     *
     *
     * Expects they metadata stored under key "key" to be of the "Set" type,
     * append "value" to the existing set or creates a new one
     */
    public void appendMetadata(String key, Object value);
    public void setMetadata(String key, Object value);
    public Optional<Object> getMetadata(String key);
}

