package com.yahoo.omid.committable.hbase;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Constants related to Commit Table
 */
public class CommitTableConstants {

    public static final String COMMIT_TABLE_NAME_KEY = "omid.committable.tablename";
    public static final String COMMIT_TABLE_DEFAULT_NAME = "OMID_COMMIT_TABLE";
    public static final byte[] COMMIT_TABLE_FAMILY = "F".getBytes(UTF_8);

    public static final byte[] LOW_WATERMARK_FAMILY = "LWF".getBytes(UTF_8);

    // Avoid instantiation
    private CommitTableConstants() {}

}
