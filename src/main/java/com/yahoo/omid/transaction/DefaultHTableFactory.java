package com.yahoo.omid.transaction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * Fabricates a standard HTable implementation to communicate with an HBase cluster as specified in the passed configuration
 *
 */
public class DefaultHTableFactory implements HTableFactory {

    @Override
    public HTableInterface create(Configuration conf, byte[] tableName) throws IOException {
        return new HTable(conf, tableName);
    }

}
