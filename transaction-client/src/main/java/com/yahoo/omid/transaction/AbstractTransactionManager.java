package com.yahoo.omid.transaction;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.transaction.RollbackException;
import com.yahoo.omid.transaction.TransactionException;
import com.yahoo.omid.transaction.Transaction.Status;
import com.yahoo.omid.tsoclient.CellId;
import com.yahoo.omid.tsoclient.TSOClient;
import com.yahoo.omid.tsoclient.TSOClient.AbortException;

/**
 * Omid's base abstract implementation of the
 * {@link TransactionManagerExtension} interface.
 *
 * Provides extra methods to allow transaction manager developers to perform
 * different actions before/after the methods exposed by the
 * {@link TransactionManager} interface.
 *
 * So, this abstract class must be extended by particular implementations of
 * transaction managers related to different storage systems (HBase...)
 */
public abstract class AbstractTransactionManager implements TransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTransactionManager.class);
    
    public enum Location {
        NOT_PRESENT, CACHE, COMMIT_TABLE, SHADOW_CELL
    }

    public class CommitTimestamp {

        private final Location location;
        private final long value;

        public CommitTimestamp(Location location, long value) {
            this.location = location;
            this.value = value;
        }

        public Location getLocation() {
            return location;
        }

        public long getValue() {
            return value;
        }

    }

    public interface TransactionFactory<T extends CellId> {

        public AbstractTransaction<T> createTransaction(long transactionId,
                AbstractTransactionManager tm);

    }

    protected final TSOClient tsoClient;
    private final boolean ownsTSOClient;
    protected final CommitTable.Client commitTableClient;
    private final boolean ownsCommitTableClient;
    private final TransactionFactory<? extends CellId> transactionFactory;
    
    /**
     * Base constructor
     *
     * @param tsoClient
     *            a client for accessing functionality of the status oracle
     * @param ownsTSOClient
     *            whether this transaction manager owns or not the TSO client
     *            instance passed. This is used to close the resources properly.
     * @param commitTableClient
     *            a client for accessing functionality of the commit table
     * @param ownsCommitTableClient
     *            whether this transaction manager owns or not the commit table
     *            client instance passed. This is used to close the resources
     *            properly.
     * @param transactionFactory
     *            a transaction factory to create the specific transaction
     *            objects required by the transaction manager being implemented.
     */
    public AbstractTransactionManager(TSOClient tsoClient,
                                      boolean ownsTSOClient,
                                      CommitTable.Client commitTableClient,
                                      boolean ownsCommitTableClient,
                                      TransactionFactory<? extends CellId> transactionFactory) {
        this.tsoClient = tsoClient;
        this.ownsTSOClient = ownsTSOClient;
        this.commitTableClient = commitTableClient;
        this.ownsCommitTableClient = ownsCommitTableClient;
        this.transactionFactory = transactionFactory;
    }
    
    /**
     * Allows specific implementations to update the shadow cells.
     * @param transaction
     *            the transaction to update shadow cells for
     * @throws TransactionManagerException
     */
    public abstract void updateShadowCells(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException;

    /**
     * Allows transaction manager developers to perform actions before
     * creating a transaction.
     * @throws TransactionManagerException
     */
    public void preBegin() throws TransactionManagerException {};

    /**
     * @see com.yahoo.omid.transaction.TransactionManager#begin()
     */
    @Override
    public final Transaction begin() throws TransactionException {

        try {
            try {
                preBegin();
            } catch (TransactionManagerException e) {
                LOG.warn(e.getMessage());
            }
            long startTimestamp = tsoClient.getNewStartTimestamp().get();
            AbstractTransaction<? extends CellId> tx =
                    transactionFactory.createTransaction(startTimestamp, this);
            try {
                postBegin(tx);
            } catch (TransactionManagerException e) {
                LOG.warn(e.getMessage());
            }
            return tx;
        } catch (ExecutionException e) {
            throw new TransactionException("Could not get new timestamp", e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException("Interrupted getting timestamp", ie);
        }
    }

    /**
     * Allows transaction manager developers to perform actions after
     * having started a transaction.
     * @param transaction
     *            the transaction that was just created.
     * @throws TransactionManagerException
     */
    public void postBegin(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException {};
    
    /**
     * Allows transaction manager developers to perform actions before
     * committing a transaction.
     * @param transaction
     *            the transaction that is going to be committed.
     * @throws TransactionManagerException
     */
    public void preCommit(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException {};
    
    /**
     * @see com.yahoo.omid.transaction.TransactionManager#commit()
     */
    @Override
    public final void commit(Transaction transaction)
            throws RollbackException, TransactionException {

        AbstractTransaction<? extends CellId> tx = enforceAbstractTransactionAsParam(transaction);
        enforceTransactionIsInRunningState(tx);

        if (tx.isRollbackOnly()) { // If the tx was marked to rollback, do it
            rollback(tx);
            throw new RollbackException("Transaction was set to rollback");
        }

        try {
            try {
                preCommit(tx);
            } catch (TransactionManagerException e) {
                throw new TransactionException(e.getMessage(), e);
            }
            long commitTs = tsoClient.commit(tx.getStartTimestamp(), tx.getWriteSet()).get();
            tx.setStatus(Status.COMMITTED);
            tx.setCommitTimestamp(commitTs);
            try {
                updateShadowCells(tx);
                // Remove transaction from commit table if not failure occurred
                commitTableClient.completeTransaction(tx.getStartTimestamp());
                postCommit(tx);
            } catch (TransactionManagerException e) {
                LOG.warn(e.getMessage());
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof AbortException) { // Conflicts detected, so rollback
                // Make sure its commit timestamp is 0, so the cleanup does the right job
                tx.setCommitTimestamp(0);
                tx.setStatus(Status.ROLLEDBACK);
                tx.cleanup();
                throw new RollbackException("Conflicts detected in tx writeset. Transaction aborted.");
            }
            throw new TransactionException("Could not commit", e.getCause());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TransactionException("Interrupted committing transaction", ie);
        }

    }

    /**
     * Allows transaction manager developers to perform actions after
     * commiting a transaction.
     * @param transaction
     *            the transaction that was committed.
     * @throws TransactionManagerException
     */
    public void postCommit(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException {};
    
    /**
     * Allows transaction manager developers to perform actions before
     * rolling-back a transaction.
     * @param transaction
     *            the transaction that is going to be rolled-back.
     * @throws TransactionManagerException
     */
    public void preRollback(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException {};

    /**
     * @see com.yahoo.omid.transaction.TransactionManager#rollback()
     */
    @Override
    public final void rollback(Transaction transaction)
            throws TransactionException {

        AbstractTransaction<? extends CellId> tx = enforceAbstractTransactionAsParam(transaction);
        enforceTransactionIsInRunningState(tx);

        try {
            try {
                preRollback(tx);
            } catch (TransactionManagerException e) {
                throw new TransactionException(e.getMessage(), e);
            }
            // Make sure its commit timestamp is 0, so the cleanup does the right job
            tx.setCommitTimestamp(0);
            tx.setStatus(Status.ROLLEDBACK);
            try {
                postRollback(tx);
            } catch (TransactionManagerException e) {
                LOG.warn(e.getMessage());
            }
        } finally {
            tx.cleanup();
        }
        
    }

    /**
     * Allows transaction manager developers to perform actions after
     * rolling-back a transaction.
     * @param transaction
     *            the transaction that was rolled-back.
     * @throws TransactionManagerException
     */
    public void postRollback(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException {};

    /**
     * Allows to find/locate the commit timestamp for a particular cell
     * if the transaction was already committed in the system. Otherwise it
     * returns a value that indicates that the commit timestamp was not found.
     * @param cellStartTimestamp
     *          start timestamp of the cell to locate the commit timestamp for.
     * @param locator
     *          a locator to find the commit timestamp in the system.
     * @return the commit timestamp joint with the location where it was found
     *         or an object indicating that it was not found in the system
     * @throws IOException 
     */
    public CommitTimestamp locateCellCommitTimestamp(long cellStartTimestamp,
            CommitTimestampLocator locator) throws IOException {
        
        try {
            // 1) First check the cache
            Optional<Long> commitTimestamp =
                    locator.readCommitTimestampFromCache(cellStartTimestamp);
            if (commitTimestamp.isPresent()) {
                return new CommitTimestamp(Location.CACHE, commitTimestamp.get());
            }
            // 2) Then check the commit table
            Future<Optional<Long>> f =
                    commitTableClient.getCommitTimestamp(cellStartTimestamp);
            commitTimestamp = f.get();
            if (commitTimestamp.isPresent()) {
                return new CommitTimestamp(Location.COMMIT_TABLE, commitTimestamp.get());
            }
            // 3) Finally, read from shadow cell
            commitTimestamp =
                    locator.readCommitTimestampFromShadowCell(cellStartTimestamp);
            if (commitTimestamp.isPresent()) {
                return new CommitTimestamp(Location.SHADOW_CELL, commitTimestamp.get());
            }
            // *) Otherwise return not found
            return new CommitTimestamp(Location.NOT_PRESENT, -1L /** TODO Check if we should return this */);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while finding commit timestamp", e);
        } catch (ExecutionException e) {
            throw new IOException("Problem finding commit timestamp", e);
        }


    }

    /**
     * @see java.io.Closeable#close()
     */
    @Override
    public final void close() throws IOException {

        if (ownsTSOClient) {
            tsoClient.close();
        }
        if (ownsCommitTableClient) {
            commitTableClient.close();
        }
        
    }
    
    // ****************************************************************************************************************
    // Helper methods
    // ****************************************************************************************************************

    private void enforceTransactionIsInRunningState(Transaction transaction) {

        if (transaction.getStatus() != Status.RUNNING) {
            throw new IllegalArgumentException("Transaction was already " + transaction.getStatus());
        }

    }

    @SuppressWarnings("unchecked")
    // NOTE: We are sure that tx is not parametrized
    private AbstractTransaction<? extends CellId>
            enforceAbstractTransactionAsParam(Transaction tx) {

        if (tx instanceof AbstractTransaction) {
            return (AbstractTransaction<? extends CellId>) tx;
        } else {
            throw new IllegalArgumentException(
                    "The transaction object passed is not an instance of AbstractTransaction");
        }

    }

}
