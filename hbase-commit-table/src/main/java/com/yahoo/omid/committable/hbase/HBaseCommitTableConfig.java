package com.yahoo.omid.committable.hbase;

import static com.yahoo.omid.committable.hbase.HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME;
import static com.yahoo.omid.committable.hbase.HBaseCommitTable.HBASE_COMMIT_TABLE_NAME_KEY;
import static com.yahoo.omid.committable.hbase.HBaseCommitTable.HBASE_COMMIT_TABLE_ENABLE_HA_KEY;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.Inject;

@Singleton
public class HBaseCommitTableConfig {

    private String tableName = COMMIT_TABLE_DEFAULT_NAME;

    private boolean enableHA = false;

    public String getTableName() {
        return tableName;
    }

    @Inject(optional = true)
    public void setTableName(@Named(HBASE_COMMIT_TABLE_NAME_KEY) String tableName) {
        this.tableName = tableName;
    }

    @Inject(optional = true)
    public void enableHA(@Named(HBASE_COMMIT_TABLE_ENABLE_HA_KEY) boolean enableHA) {
        this.enableHA = enableHA;
    }

    public boolean isHAEnabled() {
        return enableHA;
    }

}
