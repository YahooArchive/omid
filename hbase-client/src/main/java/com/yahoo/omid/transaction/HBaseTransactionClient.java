package com.yahoo.omid.transaction;

public interface HBaseTransactionClient {
    boolean isCommitted(HBaseCellId hBaseCellId) throws TransactionException;

    long getLowWatermark() throws TransactionException;
}
