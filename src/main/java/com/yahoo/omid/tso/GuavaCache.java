package com.yahoo.omid.tso;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class GuavaCache implements Cache, RemovalListener<Long, Long> {

    private static final Log LOG = LogFactory.getLog(GuavaCache.class);
    private com.google.common.cache.Cache<Long, Long> cache;
    private long removed;

    public GuavaCache(int size) {
        cache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(size).initialCapacity(size)
                .removalListener(this).build();
    }

    @Override
    public long set(long key, long value) {
        cache.put(key, value);
        // cache.cleanUp();
        return removed;
    }

    @Override
    public long get(long key) {
        Long result = cache.getIfPresent(key);
        return result == null ? 0 : result;
    }

    @Override
    public void onRemoval(RemovalNotification<Long, Long> notification) {
        if (notification.getCause() == RemovalCause.REPLACED) {
            return;
        }
//        LOG.warn("Removing " + notification);
//        new Exception().printStackTrace();
        removed = Math.max(removed, notification.getValue());
    }

}
