package com.yahoo.omid.committable;

import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class NullCommitTable implements CommitTable {
    @Override
    public ListenableFuture<CommitTable.Writer> getWriter() {
        SettableFuture<CommitTable.Writer> f = SettableFuture.<CommitTable.Writer>create();
        f.set(new Writer());
        return f;
    }

    @Override
    public ListenableFuture<CommitTable.Client> getClient() {
        SettableFuture<CommitTable.Client> f = SettableFuture.<CommitTable.Client>create();
        f.set(new Client());
        return f;
    }

    public class Writer implements CommitTable.Writer {
        @Override
        public void addCommittedTransaction(long startTimestamp, long commitTimestamp) {
            // noop
        }

        @Override
        public void updateLowWatermark(long lowWatermark) throws IOException {
            // noop
        }

        @Override
        public void clearWriteBuffer() {
            // noop
        }

        @Override
        public void flush() throws IOException {
            // noop
        }

        @Override
        public void close() {}

    }

    public static class Client implements CommitTable.Client {
        @Override
        public ListenableFuture<Optional<CommitTimestamp>> getCommitTimestamp(long startTimestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<Long> readLowWatermark() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<Void> completeTransaction(long startTimestamp) {
            SettableFuture<Void> f = SettableFuture.<Void>create();
            f.set(null);
            return f;
        }

        @Override
        public ListenableFuture<Boolean> tryInvalidateTransaction(long startTimestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
