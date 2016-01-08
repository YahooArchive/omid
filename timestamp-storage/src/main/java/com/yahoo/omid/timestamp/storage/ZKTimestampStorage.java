package com.yahoo.omid.timestamp.storage;

import static com.yahoo.omid.timestamp.storage.ZKTimestampPaths.TIMESTAMP_ZNODE;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKTimestampStorage implements TimestampStorage {

    private static final Logger LOG = LoggerFactory.getLogger(ZKTimestampStorage.class);

    public static final String DEFAULT_ZK_CLUSTER = "localhost:2181";

    static final long INITIAL_MAX_TS_VALUE = 0;

    private final CuratorFramework zkClient;

    private final DistributedAtomicLong timestamp;

    @Inject
    public ZKTimestampStorage(CuratorFramework zkClient) throws Exception {
        this.zkClient = zkClient;
        LOG.info("ZK Client state {}", zkClient.getState());
        timestamp = new DistributedAtomicLong(zkClient, TIMESTAMP_ZNODE, new RetryNTimes(3, 1000)); // TODO Configure
                                                                                                    // this?
        if (timestamp.initialize(INITIAL_MAX_TS_VALUE)) {
            LOG.info("Timestamp value in ZNode initialized to {}", INITIAL_MAX_TS_VALUE);
        }
    }

    @Override
    public void updateMaxTimestamp(long previousMaxTimestamp, long newMaxTimestamp) throws IOException {

        if (newMaxTimestamp < 0) {
            LOG.error("Negative value received for maxTimestamp: {}", newMaxTimestamp);
            throw new IllegalArgumentException();
        }
        if (newMaxTimestamp <= previousMaxTimestamp) {
            LOG.error("maxTimestamp {} <= previousMaxTimesamp: {}", newMaxTimestamp, previousMaxTimestamp);
            throw new IllegalArgumentException();
        }
        AtomicValue<Long> compareAndSet;
        try {
            compareAndSet = timestamp.compareAndSet(previousMaxTimestamp, newMaxTimestamp);
        } catch (Exception e) {
            throw new IOException("Problem setting timestamp in ZK", e);
        }
        if (!compareAndSet.succeeded()) { // We have to explicitly check for success (See Curator doc)
            throw new IOException("GetAndSet operation for storing timestamp in ZK did not succeed "
                    + compareAndSet.preValue() + " " + compareAndSet.postValue());
        }

    }

    @Override
    public long getMaxTimestamp() throws IOException {

        AtomicValue<Long> atomicValue;
        try {
            atomicValue = timestamp.get();
        } catch (Exception e) {
            throw new IOException("Problem getting data from ZK", e);
        }
        if (!atomicValue.succeeded()) { // We have to explicitly check for success (See Curator doc)
            throw new IOException("Get operation to obtain timestamp from ZK did not succeed");
        }
        return atomicValue.postValue();

    }

    // For testing
    CuratorFramework getZKClient() {
        return zkClient;
    }

}
