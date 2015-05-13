/**
 * Copyright 2011-2015 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.tso.hbase;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.timestamp.storage.TimestampStorage;

public class HBaseTimestampStorage implements TimestampStorage {

    public static final long INITIAL_MAX_TS_VALUE = 0;

    private static final Logger LOG = LoggerFactory.getLogger(HBaseTimestampStorage.class);

    public static final String TIMESTAMP_TABLE_DEFAULT_NAME = "OMID_TIMESTAMP";
    static final byte[] TSO_ROW = "MAX_TIMESTAMP_R".getBytes(UTF_8);
    public static final byte[] TSO_FAMILY = "MAX_TIMESTAMP_F".getBytes(UTF_8);
    static final byte[] TSO_QUALIFIER = "MAX_TIMESTAMP_Q".getBytes(UTF_8);

    public static final String HBASE_TIMESTAMPSTORAGE_TABLE_NAME_KEY = "omid.timestampstorage.tablename";

    private final HTable table;

    @Inject
    public HBaseTimestampStorage(Configuration hbaseConfig, HBaseTimestampStorageConfig config)
    throws IOException {
        this.table = new HTable(hbaseConfig, config.getTableName());
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
