package com.yahoo.omid.committable.hbase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.bookkeeper.client.LedgerFuture;
import org.apache.bookkeeper.client.LedgerSet.MetadataStorage;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Metadata storage implementation storing the set of ledgers in ZK
 */
public class ZKBasedMetadataStorage implements MetadataStorage {

    private static final Logger LOG = LoggerFactory.getLogger(ZKBasedMetadataStorage.class);

    private final CuratorFramework zk;
    private final ExecutorService executor;

    private volatile boolean closed = false;

    final Set<CallbackFuture<?>> outstandingCallbackFutures = new HashSet<CallbackFuture<?>>();

    @Inject
    public ZKBasedMetadataStorage(CuratorFramework zk) throws InterruptedException {
        ThreadFactoryBuilder tfb = new ThreadFactoryBuilder()
                .setNameFormat("ZKBasedMetadataStorage-%d");
        executor = Executors.newSingleThreadExecutor(tfb.build());

        this.zk = zk;
        this.zk.start();
        this.zk.blockUntilConnected();
    }

    /************************* Callback classes ******************************/

    class CallbackFuture<T> extends AbstractFuture<T> {

        CallbackFuture() {
            trackFuture(this);
        }

        void setClosedException() {
            setException(new MetadataStorage.ClosedException());
        }

        void setFatalException() {
            setException(new MetadataStorage.FatalException());
        }

    }

    class ReadCallback extends CallbackFuture<Versioned<byte[]>> implements BackgroundCallback {

        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
            int rc = event.getResultCode();
            Stat stat = event.getStat();
            byte[] data = event.getData();
            if (rc == KeeperException.Code.OK.intValue()) {
                Version version = new ZookeeperVersion(stat.getVersion());
                set(new Versioned<byte[]>(data, version));
            } else if (rc == KeeperException.Code.NONODE.intValue()) {
                setException(new MetadataStorage.NoKeyException());
            } else {
                LOG.error("Zookeeper operation failed with return code {}", rc);
                setException(new MetadataStorage.FatalException());
            }
        }

    }

    class CreateCallback extends CallbackFuture<Version> implements BackgroundCallback {

        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) {
            int rc = event.getResultCode();
            if (rc == KeeperException.Code.OK.intValue()) {
                set(new ZookeeperVersion(0));
            } else if (rc == KeeperException.Code.NODEEXISTS.intValue()) {
                setException(new MetadataStorage.BadVersionException());
            } else {
                LOG.error("Zookeeper operation failed with return code {}", rc);
                setException(new MetadataStorage.FatalException());
            }
        }

    }

    class PutCallback extends CallbackFuture<Version> implements BackgroundCallback {

        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) {
            int rc = event.getResultCode();
            if (rc == KeeperException.Code.OK.intValue()) {
                set(new ZookeeperVersion(event.getStat().getVersion()));
            } else if (rc == KeeperException.Code.BADVERSION.intValue()) {
                setException(new MetadataStorage.BadVersionException());
            } else if (rc == KeeperException.Code.NONODE.intValue()) {
                setException(new MetadataStorage.NoKeyException());
            } else {
                LOG.error("Zookeeper operation failed with return code {}", rc);
                setException(new MetadataStorage.FatalException());
            }
        }

    }

    class DeleteCallback extends CallbackFuture<Void> implements BackgroundCallback {

        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) {
            int rc = event.getResultCode();
            if (rc == KeeperException.Code.OK.intValue()) {
                set(null);
            } else if (rc == KeeperException.Code.BADVERSION.intValue()) {
                setException(new MetadataStorage.BadVersionException());
            } else if (rc == KeeperException.Code.NONODE.intValue()) {
                setException(new MetadataStorage.NoKeyException());
            } else {
                LOG.error("Zookeeper operation failed with return code {}", rc);
                setException(new MetadataStorage.FatalException());
            }
        }

    }

    void trackFuture(final CallbackFuture<?> future) {
        synchronized (outstandingCallbackFutures) {
            if (closed) {
                future.setClosedException();
                return;
            }
            outstandingCallbackFutures.add(future);
            future.addListener(new Runnable() {
                @Override
                public void run() {
                    synchronized (outstandingCallbackFutures) {
                        outstandingCallbackFutures.remove(future);
                    }
                }
            }, executor);
        }
    }

    /********************* MetadataStorage implementation ********************/

    @Override
    public synchronized LedgerFuture<Versioned<byte[]>> read(String key) {

        final String path = key;

        ReadCallback readCallback = new ReadCallback();
        try {
            zk.getData().inBackground(readCallback).forPath(path);
        } catch (Exception e) {
            LOG.error("Error accessing ZK for path " + path, e.getCause());
            readCallback.setFatalException();
        }
        return new ForwardingLedgerFuture<Versioned<byte[]>>(readCallback);
    }

    @Override
    public synchronized LedgerFuture<Version> write(String key, byte[] ledgerListAsBytes, Version version) {

        final String path = key;

        if (version == Version.NEW) {
            CreateCallback createCallback = new CreateCallback();
            try {
                zk.create().inBackground(createCallback).forPath(path, ledgerListAsBytes);
            } catch (Exception e) {
                LOG.error("Error accessing ZK for path " + path, e.getCause());
                createCallback.setFatalException();
            }
            return new ForwardingLedgerFuture<Version>(createCallback);
        } else {
            PutCallback putCallback = new PutCallback();
            int zkVersion = ((ZookeeperVersion) version).getVersion();
            try {
                zk.setData().withVersion(zkVersion).inBackground(putCallback).forPath(path, ledgerListAsBytes);
            } catch (Exception e) {
                LOG.error("Error accessing ZK for path " + path, e.getCause());
                putCallback.setFatalException();
            }
            return new ForwardingLedgerFuture<Version>(putCallback);
        }
    }

    @Override
    public synchronized LedgerFuture<Void> delete(String key, Version version) {

        final String path = key;

        final int zkVersion;
        if (version instanceof ZookeeperVersion) {
            zkVersion = ((ZookeeperVersion) version).getVersion();
        } else {
            throw new IllegalArgumentException("Invalid type for version " + version.getClass());
        }

        DeleteCallback deleteCallback = new DeleteCallback();
        try {
            zk.delete().withVersion(zkVersion).inBackground(deleteCallback).forPath(path);
        } catch (Exception e) {
            LOG.error("Error accessing ZK for path " + path, e.getCause());
            deleteCallback.setFatalException();
        }
        return new ForwardingLedgerFuture<Void>(deleteCallback);

    }

    @Override
    public synchronized void close() throws IOException {

        synchronized (outstandingCallbackFutures) {
            closed = true;
            for (CallbackFuture<?> f : outstandingCallbackFutures) {
                f.setClosedException();
            }
            outstandingCallbackFutures.clear();
        }

        executor.shutdown();

        zk.close();
    }

    /*************************** Helper classes ******************************/

    public static class ForwardingLedgerFuture<T> implements LedgerFuture<T> {
        private final ListenableFuture<T> future;

        ForwardingLedgerFuture(ListenableFuture<T> future) {
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

    public static class ZookeeperVersion implements Version {
        int version;

        public ZookeeperVersion(int v) {
            this.version = v;
        }

        public ZookeeperVersion(ZookeeperVersion v) {
            this.version = v.version;
        }

        @Override
        public Occurred compare(Version v) {
            if (null == v) {
                throw new NullPointerException("Version is not allowed to be null.");
            }
            if (v == Version.NEW) {
                return Occurred.AFTER;
            } else if (v == Version.ANY) {
                return Occurred.CONCURRENTLY;
            } else if (!(v instanceof ZookeeperVersion)) {
                throw new IllegalArgumentException("Invalid version type");
            }
            ZookeeperVersion mv = (ZookeeperVersion) v;
            int res = version - mv.version;
            if (res == 0) {
                return Occurred.CONCURRENTLY;
            } else if (res < 0) {
                return Occurred.BEFORE;
            } else {
                return Occurred.AFTER;
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (null == obj ||
                    !(obj instanceof ZookeeperVersion)) {
                return false;
            }
            ZookeeperVersion v = (ZookeeperVersion) obj;
            return 0 == (version - v.version);
        }

        @Override
        public String toString() {
            return "zkversion=" + version;
        }

        @Override
        public int hashCode() {
            return version;
        }

        public int getVersion() {
            return version;
        }

    }

}
