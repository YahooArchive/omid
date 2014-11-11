package com.yahoo.omid.committable.hbase;

import java.nio.ByteBuffer;

/**
 * A helper structure that represents the boundaries of a Tx,
 * that is, its start and commitTimestamp.
 */
public final class TxBoundaries {

    public final long startTimestamp;
    public final long commitTimestamp;

    public TxBoundaries(long startTimestamp, long commitTimestamp) {
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;
    }

    public static byte[] toBytes(long startTimestamp, long commitTimestamp) {
        ByteBuffer bb = ByteBuffer.allocate((2 * Long.SIZE) / 8);
        bb.putLong(startTimestamp);
        bb.putLong(commitTimestamp);
        return bb.array();
    }

    public static TxBoundaries fromBytes(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long startTimestamp = bb.getLong();
        long commitTimestamp = bb.getLong();
        return new TxBoundaries(startTimestamp, commitTimestamp);
    }

}

