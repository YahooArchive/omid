package com.yahoo.omid.transaction;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTableInterface;

public interface HBaseTransactionManagerIface extends TransactionManager {
    boolean isCommitted(HTableInterface table, Cell cell) throws IOException;
    long getLowWatermark() throws IOException;
}
