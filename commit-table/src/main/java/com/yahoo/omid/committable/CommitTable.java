package com.yahoo.omid.committable;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

public interface CommitTable {

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
    }
}
