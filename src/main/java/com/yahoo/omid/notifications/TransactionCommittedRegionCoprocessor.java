/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.yahoo.omid.notifications;

import static com.yahoo.omid.notifications.Constants.HBASE_META_CF;
import static com.yahoo.omid.notifications.Constants.HBASE_NOTIFY_SUFFIX;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Intercepts transactional traffic to mark changed rows for the scanners
 */
public class TransactionCommittedRegionCoprocessor extends BaseRegionObserver {

    private static final Logger logger = LoggerFactory.getLogger(TransactionCommittedRegionCoprocessor.class);

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        logger.trace("TransactionCommitterRegionObserver started");
    }

    /**
     * For each transactional write, mark the row as dirty writing to the special metadata column family
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, boolean writeToWAL)
            throws IOException {

        byte[] tableNameAsBytes = c.getEnvironment().getRegion().getTableDesc().getName();
        String tableName = Bytes.toString(tableNameAsBytes);

        // Do not instrument hbase meta tables
        if (tableName.equals(".META.") || tableName.equals("-ROOT-")) {
            // TODO allow filtering out more tables?
            return;
        }

        Map<byte[], List<KeyValue>> kvs = put.getFamilyMap();

        for (List<KeyValue> kvl : kvs.values()) {
            for (KeyValue kv : kvl) {
                String cf = Bytes.toString(kv.getFamily());

                // TODO Here we should filter the qualifiers that are not requested to be observed,
                // but cannot access to the required structures from a BaseRegionObserver
                if (!cf.equals(HBASE_META_CF)) {
                    String q = Bytes.toString(kv.getQualifier());
                    // Pattern for notify qualifier in framework's metadata column family: <cf>/<c>-notify
                    put.add(Bytes.toBytes(HBASE_META_CF), Bytes.toBytes(cf + "/" + q + HBASE_NOTIFY_SUFFIX),
                            kv.getTimestamp(), Bytes.toBytes("true"));
                }
            }
        }
    }

    /**
     * When a transaction aborts and cleans the written data, clean also the notify flags
     */
    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit,
            boolean writeToWAL) throws IOException {

        byte[] tableNameAsBytes = c.getEnvironment().getRegion().getTableDesc().getName();
        String tableName = Bytes.toString(tableNameAsBytes);

        // Do not instrument hbase meta tables
        if (tableName.equals(".META.") || tableName.equals("-ROOT-")) {
            // TODO allow filtering out more tables?
            return;
        }

        Map<byte[], List<KeyValue>> kvs = delete.getFamilyMap();

        for (List<KeyValue> kvl : kvs.values()) {
            for (KeyValue kv : kvl) {
                String cf = Bytes.toString(kv.getFamily());

                // TODO Here we should filter the qualifiers that are not requested to be observed,
                // but cannot access to the required structures from a BaseRegionObserver
                if (!cf.equals(HBASE_META_CF)) {
                    String q = Bytes.toString(kv.getQualifier());
                    // Pattern for notify qualifier in framework's metadata column family: <cf>/<c>-notify
                    delete.deleteColumn(Bytes.toBytes(HBASE_META_CF),
                            Bytes.toBytes(cf + "/" + q + HBASE_NOTIFY_SUFFIX), kv.getTimestamp());
                }
            }
        }

    }

}
