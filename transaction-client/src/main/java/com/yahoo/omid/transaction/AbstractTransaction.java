package com.yahoo.omid.transaction;

import java.util.*;

import com.google.common.base.Optional;
import com.yahoo.omid.transaction.Transaction.Status;
import com.yahoo.omid.tsoclient.CellId;

/**
 * Omid's base abstract implementation of the {@link Transaction} interface.
 * Provides extra methods to access other basic transaction state required by
 * {@link TransactionManagerExtension} implementations based on snapshot
 * isolation.
 *
 * So, this abstract class must be extended by particular implementations of
 * transaction managers related to different storage systems (HBase...)
 */
public abstract class AbstractTransaction<T extends CellId> implements Transaction {
    private transient Map<String, Object> metadata = new HashMap<String, Object>();
    private final AbstractTransactionManager transactionManager;
    private final long startTimestamp;
    private long commitTimestamp;
    private boolean isRollbackOnly;
    private final Set<T> writeSet;
    private Status status = Status.RUNNING;

    /**
     * Base constructor
     *
     * @param transactionId
     *            transaction identifier to assign
     * @param writeSet
     *            initial write set for the transaction.
     *            Should be empty in most cases.
     * @param transactionManager
     *            transaction manager associated to this transaction.
     *            Usually, should be the one that created the transaction
     *            instance.
     */
    public AbstractTransaction(long transactionId,
                               Set<T> writeSet,
                               AbstractTransactionManager transactionManager) {
        this.startTimestamp = transactionId;
        this.writeSet = writeSet;
        this.transactionManager = transactionManager;
    }

    /**
     * Allows to define specific clean-up task for transaction implementations
     */
    public abstract void cleanup();

    /**
     * @see com.yahoo.omid.transaction.Transaction#getTransactionId()
     */
    @Override
    public long getTransactionId() {
        return startTimestamp;
    }

    /**
     * @see com.yahoo.omid.transaction.Transaction#getStatus()
     */
    @Override
    public Status getStatus() {
        return status;
    }

    /**
     * @see Transaction#isRollbackOnly()
     */
    @Override
    public void setRollbackOnly() {
        isRollbackOnly = true;
    }

    /**
     * @see com.yahoo.omid.transaction.Transaction#isRollbackOnly()
     */
    @Override
    public boolean isRollbackOnly() {
        return isRollbackOnly;
    }

    /**
     * Returns transaction manager associated to this transaction.
     * @return transaction manager
     */
    public AbstractTransactionManager getTransactionManager() {
        return transactionManager;
    }
    
    /**
     * Returns the start timestamp for this transaction.
     * @return start timestamp
     */
    public long getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Returns the commit timestamp for this transaction.
     * @return commit timestamp
     */
    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    /**
     * Sets the commit timestamp for this transaction.
     * @param commitTimestamp
     *            the commit timestamp to set
     */
    public void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    /**
     * Sets the status for this transaction.
     * @param status
     *            the {@link Status} to set
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * Returns the current write-set for this transaction.
     * @return write set
     */
    public Set<T> getWriteSet() {
        return writeSet;
    }

    /**
     * Adds an element to the transaction write-set.
     * @param element
     *            the element to add
     */
    public void addWriteSetElement(T element) {
        writeSet.add(element);
    }

    public String toString() {
        return String.format("Tx-%s (ST=%d, CT=%d)",
                Long.toHexString(getTransactionId()),
                startTimestamp,
                commitTimestamp);
    }

    public Optional<Object> getMetadata(String key) {
        return Optional.fromNullable(metadata.get(key));
    }

    /**
     * Expects they metadata stored under key "key" to be of the "Set" type,
     * append "value" to the existing set or creates a new one
     */
    @SuppressWarnings("unchecked")
    public void appendMetadata(String key, Object value) {
        List existingValue = (List) metadata.get(key);
        if (existingValue == null) {
            List<Object> newList = new ArrayList<Object>();
            newList.add(value);
            metadata.put(key, newList);
        } else {
            existingValue.add(value);
        }
    }

    public void setMetadata(String key, Object value) {
        metadata.put(key, value);
    }

}