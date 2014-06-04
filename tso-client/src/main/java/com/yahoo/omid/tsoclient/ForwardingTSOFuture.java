package com.yahoo.omid.tsoclient;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Executor;
import com.google.common.util.concurrent.ListenableFuture;

class ForwardingTSOFuture<T> implements TSOFuture<T> {
    private final ListenableFuture<T> future;

    ForwardingTSOFuture(ListenableFuture<T> future) {
        this.future = future;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        future.addListener(listener, executor);
    }
}
