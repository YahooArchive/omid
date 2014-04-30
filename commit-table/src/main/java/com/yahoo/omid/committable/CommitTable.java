package com.yahoo.omid.committable;

import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

public interface CommitTable {
    ListenableFuture<Writer> getWriter();
    ListenableFuture<Client> getClient();

    public interface Writer {
        void addCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException;
        ListenableFuture<Void> flush();
    }

    public interface Client {
        ListenableFuture<Optional<Long>> getCommitTimestamp(long startTimestamp);
        ListenableFuture<Void> completeTransaction(long startTimestamp);
    }
}
