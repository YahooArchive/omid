/**
 * Copyright 2011-2016 Yahoo Inc.
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
package com.yahoo.omid.timestamp.storage;

import static com.yahoo.omid.timestamp.storage.TimestampStorage.TIMESTAMPSTORAGE_TABLE_NAME_KEY;
import static com.yahoo.omid.timestamp.storage.HBaseTimestampStorage.TIMESTAMP_TABLE_DEFAULT_NAME;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.Inject;

@Singleton
public class HBaseTimestampStorageConfig {

    private String tableName = TIMESTAMP_TABLE_DEFAULT_NAME;

    public String getTableName() {
        return tableName;
    }

    @Inject(optional = true)
    public void setTableName(@Named(TIMESTAMPSTORAGE_TABLE_NAME_KEY) String tableName) {
        this.tableName = tableName;
    }

}
