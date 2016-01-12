package com.yahoo.omid.committable.hbase;

import static com.yahoo.omid.committable.hbase.CommitTableConstants.COMMIT_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.committable.hbase.CommitTableConstants.COMMIT_TABLE_NAME_KEY;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.Inject;

@Singleton
public class HBaseCommitTableConfig {

    private String tableName = COMMIT_TABLE_DEFAULT_NAME;

    public String getTableName() {
        return tableName;
    }

    @Inject(optional = true)
    public void setTableName(@Named(COMMIT_TABLE_NAME_KEY) String tableName) {
        this.tableName = tableName;
    }

}
