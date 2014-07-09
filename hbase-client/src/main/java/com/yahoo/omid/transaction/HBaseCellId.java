package com.yahoo.omid.transaction;

import static com.google.common.base.Charsets.UTF_8;

import org.apache.hadoop.hbase.client.HTableInterface;

import com.google.common.hash.Hashing;
import com.yahoo.omid.tsoclient.CellId;

public class HBaseCellId implements CellId {

    private final HTableInterface table;
    private final byte[] row;
    private final byte[] family;
    private final byte[] qualifier;
    private long timestamp;

    public HBaseCellId(HTableInterface table, byte[] row, byte[] family, byte[] qualifier, long timestamp) {
        this.timestamp = timestamp;
        this.table = table;
        this.row = row;
        this.family = family;
        this.qualifier = qualifier;
    }

    public HTableInterface getTable() {
        return table;
    }

    public byte[] getRow() {
        return row;
    }

    public byte[] getFamily() {
        return family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String toString() {
        return new String(table.getTableName(), UTF_8)
                + ":" + new String(row, UTF_8)
                + ":" + new String(family, UTF_8)
                + ":" + new String(qualifier, UTF_8)
                + ":" + timestamp;
    }

    @Override
    public long getCellId() {
        return Hashing.murmur3_128().newHasher()
                .putBytes(table.getTableName())
                .putBytes(row)
                .putBytes(family)
                .putBytes(qualifier)
                .hash().asLong();
    }

}
