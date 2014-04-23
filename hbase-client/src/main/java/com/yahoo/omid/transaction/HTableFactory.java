package com.yahoo.omid.transaction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * Abstracts HTable implementations - used for mocking HBase in performance tests.
 *
 */
public interface HTableFactory {
    
    HTableInterface create(Configuration conf, byte[] tableName) throws IOException;

}
