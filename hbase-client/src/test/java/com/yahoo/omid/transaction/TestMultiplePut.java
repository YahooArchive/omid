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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

@Test(groups = "sharedHBase")
public class TestMultiplePut extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestMultiplePut.class);

    private static final byte[] testTable = Bytes.toBytes(TEST_TABLE);
    private static final byte[] family = Bytes.toBytes(TEST_FAMILY);
    private static final byte[] col1 = Bytes.toBytes("col1");
    private static final byte[] col2 = Bytes.toBytes("col2");
    private static final byte[] data = Bytes.toBytes("testData");

    @Test(timeOut = 30_000)
    public void testMultiPutInTwoDifferentColsOfSameRowAreInTheTableAfterCommit(ITestContext context) throws Exception {

        TransactionManager tm = newTransactionManager(context);

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            Transaction tx = tm.begin();

            byte[] rowToAdd = Bytes.toBytes(1000);

            Put put1 = new Put(rowToAdd);
            put1.add(family, col1, data);
            txTable.put(tx, put1);

            Put put2 = new Put(rowToAdd);
            put2.add(family, col2, data);
            txTable.put(tx, put2);

            tm.commit(tx);

            assertTrue(verifyValue(testTable, rowToAdd, family, col1, data), "Invalid value in table");
            assertTrue(verifyValue(testTable, rowToAdd, family, col2, data), "Invalid value in table");
        }

    }

    @Test(timeOut = 30_000)
    public void testManyManyPutsInDifferentRowsAreInTheTableAfterCommit(ITestContext context) throws Exception {

        final int NUM_ROWS_TO_ADD = 50;

        TransactionManager tm = newTransactionManager(context);

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            Transaction tx = tm.begin();

            for (int i = 0; i <= NUM_ROWS_TO_ADD; i++) {
                byte[] rowToAdd = Bytes.toBytes(i);
                byte[] dataForRowCol = Bytes.toBytes("testData" + i);
                Put put = new Put(rowToAdd);
                put.add(family, col1, dataForRowCol);
                txTable.put(tx, put);
            }

            tm.commit(tx);

            // Check some of the added values are there in the table
            byte[] rowToCheck = Bytes.toBytes(0);
            byte[] dataToCheck = Bytes.toBytes("testData" + 0);
            assertTrue(verifyValue(testTable, rowToCheck, family, col1, dataToCheck), "Invalid value in table");
            rowToCheck = Bytes.toBytes(NUM_ROWS_TO_ADD / 2);
            dataToCheck = Bytes.toBytes("testData" + (NUM_ROWS_TO_ADD / 2));
            assertTrue(verifyValue(testTable, rowToCheck, family, col1, dataToCheck), "Invalid value in table");
            rowToCheck = Bytes.toBytes(NUM_ROWS_TO_ADD);
            dataToCheck = Bytes.toBytes("testData" + NUM_ROWS_TO_ADD);
            assertTrue(verifyValue(testTable, rowToCheck, family, col1, dataToCheck), "Invalid value in table");

        }
    }

    @Test(timeOut = 30_000)
    public void testGetFromNonExistentRowAfterMultiplePutsReturnsNoResult(ITestContext context) throws Exception {

        final int NUM_ROWS_TO_ADD = 10;

        TransactionManager tm = newTransactionManager(context);

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            Transaction tx = tm.begin();

            for (int i = 0; i < NUM_ROWS_TO_ADD; i++) {
                byte[] rowToAdd = Bytes.toBytes(i);
                Put put = new Put(rowToAdd);
                put.add(family, col1, Bytes.toBytes("testData" + i));
                txTable.put(tx, put);
            }

            byte[] nonExistentRow = Bytes.toBytes(NUM_ROWS_TO_ADD + 5);
            Get get = new Get(nonExistentRow);
            Result result = txTable.get(tx, get);

            assertTrue(result.isEmpty(), "Found a row that should not exist");

            tm.commit(tx);

        }

    }

}
