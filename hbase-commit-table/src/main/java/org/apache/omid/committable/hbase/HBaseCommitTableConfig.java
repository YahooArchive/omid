/*
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
package org.apache.omid.committable.hbase;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.util.Bytes;

import static com.google.common.base.Charsets.UTF_8;

public class HBaseCommitTableConfig {

    public static final String COMMIT_TABLE_NAME_KEY = "omid.committable.tablename";
    public static final String COMMIT_TABLE_CF_NAME_KEY = "omid.committable.cfname";
    public static final String COMMIT_TABLE_LWM_CF_NAME_KEY = "omid.committable.lwm.cfname";

    public static final String DEFAULT_COMMIT_TABLE_NAME = "OMID_COMMIT_TABLE";
    public static final String DEFAULT_COMMIT_TABLE_CF_NAME = "F";
    public static final String DEFAULT_COMMIT_TABLE_LWM_CF_NAME = "LWF";

    static final byte[] COMMIT_TABLE_QUALIFIER = "C".getBytes(UTF_8);
    static final byte[] INVALID_TX_QUALIFIER = "IT".getBytes(UTF_8);
    static final byte[] LOW_WATERMARK_QUALIFIER = "LWC".getBytes(UTF_8);
    static final byte[] LOW_WATERMARK_ROW = "LOW_WATERMARK".getBytes(UTF_8);

    // ----------------------------------------------------------------------------------------------------------------
    // Configuration parameters
    // ----------------------------------------------------------------------------------------------------------------

    private String tableName = DEFAULT_COMMIT_TABLE_NAME;
    private byte[] commitTableFamily = Bytes.toBytes(DEFAULT_COMMIT_TABLE_CF_NAME);
    private byte[] lowWatermarkFamily = Bytes.toBytes(DEFAULT_COMMIT_TABLE_LWM_CF_NAME);

    // ----------------------------------------------------------------------------------------------------------------
    // Getters and setters
    // ----------------------------------------------------------------------------------------------------------------

    public String getTableName() {
        return tableName;
    }


    @Inject(optional = true)
    public void setTableName(@Named(COMMIT_TABLE_NAME_KEY) String tableName) {
        this.tableName = tableName;
    }

    public byte[] getCommitTableFamily() {
        return commitTableFamily;
    }


    @Inject(optional = true)
    public void setCommitTableFamily(@Named(COMMIT_TABLE_CF_NAME_KEY) String commitTableFamily) {
        this.commitTableFamily = commitTableFamily.getBytes(UTF_8);
    }

    public byte[] getLowWatermarkFamily() {
        return lowWatermarkFamily;
    }

    @Inject(optional = true)
    public void setLowWatermarkFamily(@Named(COMMIT_TABLE_LWM_CF_NAME_KEY) String lowWatermarkFamily) {
        this.lowWatermarkFamily = lowWatermarkFamily.getBytes(UTF_8);
    }

}
