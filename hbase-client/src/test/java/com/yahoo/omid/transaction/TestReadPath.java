/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.transaction;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;

public class TestReadPath extends OmidTestBase {

    @Test
    public void testReadWithSeveralUncommitted() throws Exception {
        byte[] family = Bytes.toBytes(TEST_FAMILY);
        byte[] row = Bytes.toBytes("row");
        byte[] col = Bytes.toBytes("col1");
        byte[] data = Bytes.toBytes("data");
        byte[] uncommitted = Bytes.toBytes("uncommitted");
        TransactionManager tm = newTransactionManager();
        TTable table = new TTable(hbaseConf, TEST_TABLE);
        
        // Put some data on the DB
        Transaction t = tm.begin();
        Put put = new Put(row);
        put.add(family, col, data);
        table.put(t, put);
        tm.commit(t);
        List<Transaction> running = new ArrayList<Transaction>();

        // Shade the data with uncommitted data
        for (int i = 0; i < 10; ++i) {
            t = tm.begin();
            put = new Put(row);
            put.add(family, col, uncommitted);
            table.put(t, put);
            running.add(t);
        }

        // Try to read from row, it should ignore the uncommitted data and return the original committed value
        t = tm.begin();
        Get get = new Get(row);
        Result result = table.get(t, get);
        KeyValue kv = result.getColumnLatest(family, col);
        assertNotNull("KeyValue is null", kv);
        byte[] value = kv.getValue();
        assertTrue("Read data doesn't match", Arrays.equals(data, value));
        tm.commit(t);

        table.close();

        for (Transaction r : running) {
            tm.rollback(r);
        }

    }

}
