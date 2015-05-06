package com.yahoo.omid.committable;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

public interface CommitTable {

    public static final long INVALID_TRANSACTION_MARKER = -1L;

    ListenableFuture<Writer> getWriter();
    ListenableFuture<Client> getClient();

    public interface Writer extends Closeable {
        void addCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException;
        void updateLowWatermark(long lowWatermark) throws IOException;
        // TODO Make this synchronous
        ListenableFuture<Void> flush();
    }

    public interface Client extends Closeable {
        ListenableFuture<Optional<Long>> getCommitTimestamp(long startTimestamp);
        ListenableFuture<Long> readLowWatermark();
        ListenableFuture<Void> completeTransaction(long startTimestamp);
        /**
         * Atomically tries to invalidate a non-committed transaction launched
         * by a previous TSO server.
         * @param startTimeStamp
         *              the transaction to invalidate
         * @return true on success and false on failure
         */
        ListenableFuture<Boolean> tryInvalidateTransaction(long startTimeStamp);
    }
}
