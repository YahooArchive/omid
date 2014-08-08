package com.yahoo.omid.transaction;

import static org.testng.AssertJUnit.assertTrue;
import org.testng.annotations.Test;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;

public class TestSingleColumnFamily extends OmidTestBase {
   private static final Logger LOG = LoggerFactory.getLogger(TestSingleColumnFamily.class);


    @Test
    public void testSingleColumnFamily() throws Exception {
        TransactionManager tm = newTransactionManager();
        TTable table1 = new TTable(hbaseConf, TEST_TABLE);
        int num=10;
        Transaction t=tm.begin();
        for(int j=0;j<num;j++) {
            byte[]data=Bytes.toBytes(j);
            Put put=new Put(data);
            put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value1"), data);
            put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value2"), data);
            table1.put(t,put);
        }
        //tm.tryCommit(t);
        //t=tm.beginTransaction(); //Visible if in a different transaction
        Scan s=new Scan();
        ResultScanner res=table1.getScanner(t,s);
        Result rr;
        int count = 0;
        while ((rr=res.next())!=null) {
            int tmp1=Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value1")));
            int tmp2=Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value2")));
            LOG.info("RES:"+tmp1+";"+tmp2);
            count++;
        }
        assertTrue("Can't see puts. I should see " 
                   + num + " but I see " + count
                   , num == count);

        tm.commit(t);
        t=tm.begin();

        for(int j=0;j<num/2;j++) {
            byte[]data=Bytes.toBytes(j);
            byte[]ndata=Bytes.toBytes(j*10);
            Put put=new Put(data);
            put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value2"), ndata);
            table1.put(t,put);
        }
        tm.commit(t);
        t=tm.begin();
        s=new Scan();
        res=table1.getScanner(t,s);
        count = 0;
        int modified = 0, notmodified = 0;
        while ((rr=res.next())!=null) {
            int tmp1=Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value1")));
            int tmp2=Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value2")));
            LOG.info("RES:"+tmp1+";"+tmp2);
            count++;
         
            if (tmp2 == Bytes.toInt(rr.getRow())*10) {
                modified++;
            } else {
                notmodified++;
            }
            if (count == 8) {
                LOG.debug("stop");
            }
        }
        assertTrue("Can't see puts. I should see " 
                   + num + " but I see " + count
                   , num == count);
        assertTrue("Half of rows should equal row id, half not (" 
                   + modified + ", " + notmodified + ")"
                   , modified == notmodified && notmodified == (num/2));
      
        tm.commit(t);
        LOG.info("End commiting");
        table1.close();
    }
}
