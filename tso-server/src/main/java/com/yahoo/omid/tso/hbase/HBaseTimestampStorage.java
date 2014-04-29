package com.yahoo.omid.tso.hbase;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.tso.TimestampOracle.TimestampStorage;

public class HBaseTimestampStorage implements TimestampStorage {

    public static final long INITIAL_MAX_TS_VALUE = 0;

    private static final Logger LOG = LoggerFactory.getLogger(HBaseTimestampStorage.class);

    static final byte[] TSO_ROW = "MAX_TIMESTAMP_R".getBytes(UTF_8);
    static final byte[] TSO_FAMILY = "MAX_TIMESTAMP_F".getBytes(UTF_8);
    static final byte[] TSO_QUALIFIER = "MAX_TIMESTAMP_Q".getBytes(UTF_8);

    private final HTable table;

    public HBaseTimestampStorage(HTable table) {
        this.table = table;
    }

    @Override
    public void updateMaxTimestamp(long previousMaxTimestamp, long newMaxTimestamp) throws IOException {
        if (newMaxTimestamp < 0) {
            LOG.error("Negative value received for maxTimestamp: {}", newMaxTimestamp);
            throw new IllegalArgumentException();
        }
        Put put = new Put(TSO_ROW);
        put.add(TSO_FAMILY, TSO_QUALIFIER, Bytes.toBytes(newMaxTimestamp));
        byte[] previousVal = null;
        if (previousMaxTimestamp != INITIAL_MAX_TS_VALUE) {
            previousVal = Bytes.toBytes(previousMaxTimestamp);
        }
        if (!table.checkAndPut(TSO_ROW, TSO_FAMILY, TSO_QUALIFIER, previousVal, put)) {
            throw new IOException("Previous max timestamp is incorrect");
        }
    }

    @Override
    public long getMaxTimestamp() throws IOException {
        Get get = new Get(TSO_ROW);
        get.addColumn(TSO_FAMILY, TSO_QUALIFIER);

        Result result = table.get(get);
        if (result.containsColumn(TSO_FAMILY, TSO_QUALIFIER)) {
            return Bytes.toLong(result.getValue(TSO_FAMILY, TSO_QUALIFIER));
        } else { // This happens for example when a new cluster is created
            return INITIAL_MAX_TS_VALUE;
        }

    }

}
