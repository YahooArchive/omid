package com.yahoo.omid.transaction;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;

public class TestMultiplePut extends OmidTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TestMultiplePut.class);

    
    @Test 
    public void testMultiPutSameRow() throws Exception {
        try{
            byte[] family = Bytes.toBytes(TEST_FAMILY);
            byte[] col1 = Bytes.toBytes("value1");
            byte[] col2 = Bytes.toBytes("value2");
            TransactionManager tm = newTransactionManager();
            TTable table1 = new TTable(hbaseConf, TEST_TABLE);
            Transaction t=tm.begin();
            int val=1000;
            byte[]data=Bytes.toBytes(val);
            Put put1=new Put(data);
            put1.add(family, col1, data);
            table1.put(t,put1);
            Put put2=new Put(data);
            put2.add(family, col2, data);
            table1.put(t,put2);
            tm.commit(t);
            table1.close();

            assertTrue("Invalid value in table", verifyValue(Bytes.toBytes(TEST_TABLE),
                                                             data, family, col1, data));
            assertTrue("Invalid value in table", verifyValue(Bytes.toBytes(TEST_TABLE),
                                                             data, family, col2, data));
        } catch (Exception e) {
            LOG.error("Exception in test", e);
            throw e;
        }
    }

    @Test 
    public void testManyManyPut() throws Exception {
        try{
            byte[] family = Bytes.toBytes(TEST_FAMILY);
            byte[] col = Bytes.toBytes("value");

            TransactionManager tm = newTransactionManager();
            TTable table1 = new TTable(hbaseConf, TEST_TABLE);
            Transaction t=tm.begin();
            int num=50;
            for(int j=0;j<=num;j++) {
                byte[]data=Bytes.toBytes(j);
                Put put=new Put(data);
                put.add(family, col, data);
                table1.put(t,put);
            }
            tm.commit(t);
            table1.close();

            byte[] data=Bytes.toBytes(0);
            assertTrue("Invalid value in table", verifyValue(Bytes.toBytes(TEST_TABLE),
                                                             data, family, col, data));
            data=Bytes.toBytes(num/2);
            assertTrue("Invalid value in table", verifyValue(Bytes.toBytes(TEST_TABLE),
                                                             data, family, col, data));
            data=Bytes.toBytes(num);
            assertTrue("Invalid value in table", verifyValue(Bytes.toBytes(TEST_TABLE),
                                                             data, family, col, data));
        } catch (Exception e) {
            LOG.error("Exception in test", e);
            throw e;
        }
    }   
}
