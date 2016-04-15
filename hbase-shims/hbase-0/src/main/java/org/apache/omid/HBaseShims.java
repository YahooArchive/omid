/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.omid;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;

import java.io.IOException;

public class HBaseShims {

    static public void setKeyValueSequenceId(KeyValue kv, int sequenceId) {

        kv.setMvccVersion(sequenceId);

    }

    static public Region getRegionCoprocessorRegion(RegionCoprocessorEnvironment env) {

        return new Region(env.getRegion());

    }

    static public void flushAllOnlineRegions(HRegionServer regionServer, TableName tableName) throws IOException {

        for (HRegion r : regionServer.getOnlineRegions(tableName)) {
            r.flushcache();
        }

    }

    static public void addFamilyToHTableDescriptor(HTableDescriptor tableDesc, HColumnDescriptor columnDesc) {

        tableDesc.addFamily(columnDesc);

    }

}