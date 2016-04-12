/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.transaction;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "sharedHBase")
public class TestSingleColumnFamily extends OmidTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TestSingleColumnFamily.class);


    @Test
    public void testSingleColumnFamily(ITestContext context) throws Exception {
        TransactionManager tm = newTransactionManager(context);
        TTable table1 = new TTable(hbaseConf, TEST_TABLE);
        int num = 10;
        Transaction t = tm.begin();
        for (int j = 0; j < num; j++) {
            byte[] data = Bytes.toBytes(j);
            Put put = new Put(data);
            put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value1"), data);
            put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value2"), data);
            table1.put(t, put);
        }
        //tm.tryCommit(t);
        //t=tm.beginTransaction(); //Visible if in a different transaction
        Scan s = new Scan();
        ResultScanner res = table1.getScanner(t, s);
        Result rr;
        int count = 0;
        while ((rr = res.next()) != null) {
            int tmp1 = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value1")));
            int tmp2 = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value2")));
            LOG.info("RES:" + tmp1 + ";" + tmp2);
            count++;
        }
        assertTrue("Can't see puts. I should see "
                        + num + " but I see " + count
                , num == count);

        tm.commit(t);
        t = tm.begin();

        for (int j = 0; j < num / 2; j++) {
            byte[] data = Bytes.toBytes(j);
            byte[] ndata = Bytes.toBytes(j * 10);
            Put put = new Put(data);
            put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value2"), ndata);
            table1.put(t, put);
        }
        tm.commit(t);
        t = tm.begin();
        s = new Scan();
        res = table1.getScanner(t, s);
        count = 0;
        int modified = 0, notmodified = 0;
        while ((rr = res.next()) != null) {
            int tmp1 = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value1")));
            int tmp2 = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value2")));
            LOG.info("RES:" + tmp1 + ";" + tmp2);
            count++;

            if (tmp2 == Bytes.toInt(rr.getRow()) * 10) {
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
                , modified == notmodified && notmodified == (num / 2));

        tm.commit(t);
        LOG.info("End commiting");
        table1.close();
    }
}
