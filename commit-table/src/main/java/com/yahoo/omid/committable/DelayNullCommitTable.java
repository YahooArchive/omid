package com.yahoo.omid.committable;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;

/**
 * Testing commit table to simulate the delay of writing
 * to remove storage
 */
public class DelayNullCommitTable implements CommitTable {
    final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
            .setNameFormat("delay-commit-%d").build());

    final long delay;
    final TimeUnit unit;

    public DelayNullCommitTable(long delay, TimeUnit unit) {
        this.delay = delay;
        this.unit = unit;
    }

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
        public ListenableFuture<Void> flush() {
            final SettableFuture<Void> f = SettableFuture.<Void>create();
            executor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        f.set(null);
                    }
                }, delay, unit);

            return f;
        }

        @Override
        public void close() {}
    }

    public class Client implements CommitTable.Client {
        @Override
        public ListenableFuture<Optional<Long>> getCommitTimestamp(long startTimestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<Void> completeTransaction(long startTimestamp) {
            SettableFuture<Void> f = SettableFuture.<Void>create();
            f.set(null);
            return f;
        }

        @Override
        public void close() {}
    }
}
