package com.yahoo.omid.transaction;

import java.io.IOException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.KeyValue;

public interface HBaseTransactionManagerIface extends TransactionManager {
    boolean isCommitted(HTableInterface table, KeyValue kv) throws IOException;
    long getLowWatermark() throws IOException;
}
