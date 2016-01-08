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
