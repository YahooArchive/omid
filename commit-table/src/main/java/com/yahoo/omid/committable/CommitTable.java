package com.yahoo.omid.committable;

import com.google.common.util.concurrent.ListenableFuture;

public interface CommitTable {
    ListenableFuture<Writer> getWriter();
    ListenableFuture<Client> getClient();

    public interface Writer {
        void addCommittedTransaction(long startTimestamp, long commitTimestamp);
        ListenableFuture<Void> flush();
    }

    public interface Client {
        ListenableFuture<Long> getCommitTimestamp(long startTimestamp);
        ListenableFuture<Void> completeTransaction(long startTimestamp);
    }
}
