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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class TransactionCommittedRegionObserver extends BaseRegionObserver {

    private static final Logger logger = Logger.getLogger(TransactionCommittedRegionObserver.class);

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        logger.trace("TransactionCommitterRegionObserver started");
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, boolean writeToWAL)
            throws IOException {

        byte[] tableNameAsBytes = c.getEnvironment().getRegion().getTableDesc().getName();
        String tableName = Bytes.toString(tableNameAsBytes);

        //logger.trace("We're in pre-put");
        HTable table = new HTable(HBaseConfiguration.create(), tableName);

        Map<byte[], List<KeyValue>> kvs = put.getFamilyMap();

        for (List<KeyValue> kvl : kvs.values()) {
            for (KeyValue kv : kvl) {
                String cf = Bytes.toString(kv.getFamily());
                // TODO Here we sould filter the columns that are not requested to be observed, 
                // but cannot access to the required structures from a BaseRegionObserver
                if(!cf.endsWith(Constants.NOTIF_HBASE_CF_SUFFIX)) {
                    String col = Bytes.toString(kv.getQualifier());
                    put.add(Bytes.toBytes(cf + Constants.NOTIF_HBASE_CF_SUFFIX),
                            Bytes.toBytes(col + Constants.HBASE_NOTIFY_SUFFIX), Bytes.toBytes("true"));
                    logger.trace("This is the put added : " + put);
                }
            }
        }
        table.close();
        //logger.trace("Out of pre-put");
    }

}
