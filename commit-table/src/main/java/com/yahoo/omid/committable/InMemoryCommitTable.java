package com.yahoo.omid.committable;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.ConcurrentHashMap;

public class InMemoryCommitTable implements CommitTable {
    final ConcurrentHashMap<Long, Long> table = new ConcurrentHashMap<Long, Long>();

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
            table.put(startTimestamp, commitTimestamp);
        }

        @Override
        public ListenableFuture<Void> flush() {
            SettableFuture<Void> f = SettableFuture.<Void>create();
            f.set(null);
            return f;
        }
    }

    public class Client implements CommitTable.Client {
        @Override
        public ListenableFuture<Long> getCommitTimestamp(long startTimestamp) {
            SettableFuture<Long> f = SettableFuture.<Long>create();
            f.set(table.get(startTimestamp));
            return f;
        }

        @Override
        public ListenableFuture<Void> completeTransaction(long startTimestamp) {
            SettableFuture<Void> f = SettableFuture.<Void>create();
            table.remove(startTimestamp);
            f.set(null);
            return f;
        }
    }
}
