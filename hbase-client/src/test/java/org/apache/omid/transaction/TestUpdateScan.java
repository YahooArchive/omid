/*
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
package org.apache.omid.transaction;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = "sharedHBase")
public class TestUpdateScan extends OmidTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(TestUpdateScan.class);

    private static final String TEST_COL = "value";
    private static final String TEST_COL_2 = "col_2";

    @Test
    public void testGet(ITestContext context) throws Exception {
        try {
            TransactionManager tm = newTransactionManager(context);
            TTable table = new TTable(hbaseConf, TEST_TABLE);
            Transaction t = tm.begin();
            int[] lInts = new int[]{100, 243, 2342, 22, 1, 5, 43, 56};
            for (int i = 0; i < lInts.length; i++) {
                byte[] data = Bytes.toBytes(lInts[i]);
                Put put = new Put(data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL), data);
                table.put(t, put);
            }
            int startKeyValue = lInts[3];
            int stopKeyValue = lInts[3];
            byte[] startKey = Bytes.toBytes(startKeyValue);
            byte[] stopKey = Bytes.toBytes(stopKeyValue);
            Get g = new Get(startKey);
            Result r = table.get(t, g);
            if (!r.isEmpty()) {
                int tmp = Bytes.toInt(r.getValue(Bytes.toBytes(TEST_FAMILY),
                        Bytes.toBytes(TEST_COL)));
                LOG.info("Result:" + tmp);
                assertTrue(tmp == startKeyValue, "Bad value, should be " + startKeyValue + " but is " + tmp);
            } else {
                Assert.fail("Bad result");
            }
            tm.commit(t);

            Scan s = new Scan(startKey);
            CompareFilter.CompareOp op = CompareFilter.CompareOp.LESS_OR_EQUAL;
            RowFilter toFilter = new RowFilter(op, new BinaryPrefixComparator(stopKey));
            boolean startInclusive = true;
            if (!startInclusive) {
                FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filters.addFilter(new RowFilter(CompareFilter.CompareOp.GREATER,
                        new BinaryPrefixComparator(startKey)));
                filters.addFilter(new WhileMatchFilter(toFilter));
                s.setFilter(filters);
            } else {
                s.setFilter(new WhileMatchFilter(toFilter));
            }
            t = tm.begin();
            ResultScanner res = table.getScanner(t, s);
            Result rr;
            int count = 0;
            while ((rr = res.next()) != null) {
                int iTmp = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY),
                        Bytes.toBytes(TEST_COL)));
                LOG.info("Result: " + iTmp);
                count++;
            }
            assertEquals(count, 1, "Count is wrong");
            LOG.info("Rows found " + count);
            tm.commit(t);
            table.close();
        } catch (Exception e) {
            LOG.error("Exception in test", e);
        }
    }

    @Test
    public void testScan(ITestContext context) throws Exception {

        try (TTable table = new TTable(hbaseConf, TEST_TABLE)) {
            TransactionManager tm = newTransactionManager(context);
            Transaction t = tm.begin();
            int[] lInts = new int[]{100, 243, 2342, 22, 1, 5, 43, 56};
            for (int lInt : lInts) {
                byte[] data = Bytes.toBytes(lInt);
                Put put = new Put(data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL), data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL_2), data);
                table.put(t, put);
            }

            Scan s = new Scan();
            // Adding two columns to the scanner should not throw a
            // ConcurrentModificationException when getting the scanner
            s.addColumn(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL));
            s.addColumn(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL_2));
            ResultScanner res = table.getScanner(t, s);
            Result rr;
            int count = 0;
            while ((rr = res.next()) != null) {
                int iTmp = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY),
                        Bytes.toBytes(TEST_COL)));
                LOG.info("Result: " + iTmp);
                count++;
            }
            assertTrue(count == lInts.length, "Count should be " + lInts.length + " but is " + count);
            LOG.info("Rows found " + count);

            tm.commit(t);

            t = tm.begin();
            res = table.getScanner(t, s);
            count = 0;
            while ((rr = res.next()) != null) {
                int iTmp = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY),
                        Bytes.toBytes(TEST_COL)));
                LOG.info("Result: " + iTmp);
                count++;
            }
            assertTrue(count == lInts.length, "Count should be " + lInts.length + " but is " + count);
            LOG.info("Rows found " + count);
            tm.commit(t);
        }

    }


    @Test
    public void testScanUncommitted(ITestContext context) throws Exception {
        try {
            TransactionManager tm = newTransactionManager(context);
            TTable table = new TTable(hbaseConf, TEST_TABLE);
            Transaction t = tm.begin();
            int[] lIntsA = new int[]{100, 243, 2342, 22, 1, 5, 43, 56};
            for (int aLIntsA : lIntsA) {
                byte[] data = Bytes.toBytes(aLIntsA);
                Put put = new Put(data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL), data);
                table.put(t, put);
            }
            tm.commit(t);

            Transaction tu = tm.begin();
            int[] lIntsB = new int[]{105, 24, 4342, 32, 7, 3, 30, 40};
            for (int aLIntsB : lIntsB) {
                byte[] data = Bytes.toBytes(aLIntsB);
                Put put = new Put(data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL), data);
                table.put(tu, put);
            }

            t = tm.begin();
            int[] lIntsC = new int[]{109, 224, 242, 2, 16, 59, 23, 26};
            for (int aLIntsC : lIntsC) {
                byte[] data = Bytes.toBytes(aLIntsC);
                Put put = new Put(data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL), data);
                table.put(t, put);
            }
            tm.commit(t);

            t = tm.begin();
            Scan s = new Scan();
            ResultScanner res = table.getScanner(t, s);
            Result rr;
            int count = 0;

            while ((rr = res.next()) != null) {
                int iTmp = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY),
                        Bytes.toBytes(TEST_COL)));
                LOG.info("Result: " + iTmp);
                count++;
            }
            assertTrue(count == lIntsA.length + lIntsC.length,
                       "Count should be " + (lIntsA.length * lIntsC.length) + " but is " + count);
            LOG.info("Rows found " + count);
            tm.commit(t);
            table.close();
        } catch (Exception e) {
            LOG.error("Exception in test", e);
        }
    }
}
