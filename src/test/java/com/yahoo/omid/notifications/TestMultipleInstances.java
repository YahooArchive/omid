package com.yahoo.omid.notifications;

import static com.yahoo.omid.examples.Constants.COLUMN_1;
import static com.yahoo.omid.examples.Constants.COLUMN_FAMILY_1;
import static com.yahoo.omid.examples.Constants.TABLE_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.test.TestingServer;
import com.yahoo.omid.notifications.client.DeltaOmid;
import com.yahoo.omid.notifications.client.IncrementalApplication;
import com.yahoo.omid.notifications.client.Observer;
import com.yahoo.omid.notifications.conf.ClientConfiguration;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;

public class TestMultipleInstances extends TestInfrastructure {
    private static Logger logger = LoggerFactory.getLogger(TestDeltaOmid.class);
    
    private class ObserverLatch implements Observer {
        CountDownLatch latch;
        ObserverLatch (CountDownLatch latch) {
            this.latch = latch;
        }
        @Override
        public void onInterestChanged(Result rowData, Transaction tx) throws IOException {
            latch.countDown();
        }
        @Override
        public String getName() {
            return "obs";
        }
        @Override
        public Interest getInterest() {
            return new Interest(TABLE_1, COLUMN_FAMILY_1, COLUMN_1);
        }
        
    }

    @Test
    public void testTwoInstances() throws Exception {
        byte[] value = Bytes.toBytes("value");
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        final IncrementalApplication app1 = new DeltaOmid.AppBuilder("TestApp", 6666)
                .addObserver(new ObserverLatch(latch1)).build();
        final IncrementalApplication app2 = new DeltaOmid.AppBuilder("TestApp", 6667)
                .addObserver(new ObserverLatch(latch2)).build();

        Thread.sleep(5000);

        TransactionManager tm = new TransactionManager(hbaseConf);
        TTable tt = new TTable(hbaseConf, TestConstants.TABLE);

        for (int i = 0; i < 200; ++i) {
            Transaction tx = tm.begin();

            Put row = new Put(Bytes.toBytes(i));
            row.add(Bytes.toBytes(COLUMN_FAMILY_1), Bytes.toBytes(COLUMN_1), value);
            tt.put(tx, row);

            tm.commit(tx);
        }

        tt.close();

        assertTrue("App1 didn't receive notifications", latch1.await(15, TimeUnit.SECONDS));
        assertTrue("App2 didn't receive notifications", latch2.await(15, TimeUnit.SECONDS));

        app1.close();
        app2.close();
    }

}
