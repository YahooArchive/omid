package com.yahoo.omid.transaction;

import java.io.Closeable;

/**
 * Provides the methods to manage transactions (create, commit...)
 */
public interface TransactionManager extends Closeable {

    /**
     * Starts a new transaction.
     * 
     * Creates & returns a {@link Transaction} interface implementation
     * that will be used in {@link TTable}'s methods for doing operations
     * on the transactional context defined by the returned object.
     * 
     * @return transaction
     *          representation of the created transaction
     * @throws TransactionException
     */
    public Transaction begin() throws TransactionException;
    
    /**
     * Commits a transaction.
     * 
     * If the transaction was marked for rollback or has conflicts
     * with another concurrent transaction it will be rolledback
     * automatically and a {@link RollbackException} will be thrown.
     * 
     * @param tx
     *          transaction to be committed.
     * @throws RollbackException
     *          thrown when transaction has conflicts with another transaction
     *          or when was marked for rollback.
     * @throws TransactionException
     */
    public void commit(Transaction tx) throws RollbackException, TransactionException;
    
    /**
     * Aborts a transaction.
     * 
     * Automatically rollbacks the changes performed by the transaction.
     * 
     * @param tx
     *          transaction to be rolled-back.
     */
    public void rollback(Transaction tx) throws TransactionException;
    
}
