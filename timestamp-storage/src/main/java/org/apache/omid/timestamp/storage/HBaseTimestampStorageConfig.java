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
package org.apache.omid.timestamp.storage;

import com.google.inject.Inject;

import javax.inject.Named;

public class HBaseTimestampStorageConfig {

    public static final String TIMESTAMP_STORAGE_TABLE_NAME_KEY = "omid.timestampstorage.tablename";
    public static final String TIMESTAMP_STORAGE_CF_NAME_KEY = "omid.timestampstorage.cfname";

    public static final String DEFAULT_TIMESTAMP_STORAGE_TABLE_NAME = "OMID_TIMESTAMP_TABLE";
    public static final String DEFAULT_TIMESTAMP_STORAGE_CF_NAME = "MAX_TIMESTAMP_CF";

    // ----------------------------------------------------------------------------------------------------------------
    // Configuration parameters
    // ----------------------------------------------------------------------------------------------------------------

    private String tableName = DEFAULT_TIMESTAMP_STORAGE_TABLE_NAME;
    private String familyName = DEFAULT_TIMESTAMP_STORAGE_CF_NAME;

    // ----------------------------------------------------------------------------------------------------------------
    // Getters and setters
    // ----------------------------------------------------------------------------------------------------------------

    public String getTableName() {
        return tableName;
    }

    @Inject(optional = true)
    public void setTableName(@Named(TIMESTAMP_STORAGE_TABLE_NAME_KEY) String tableName) {
        this.tableName = tableName;
    }

    public String getFamilyName() {
        return familyName;
    }

    @Inject(optional = true)
    public void setFamilyName(@Named(TIMESTAMP_STORAGE_CF_NAME_KEY) String familyName) {
        this.familyName = familyName;
    }
}
