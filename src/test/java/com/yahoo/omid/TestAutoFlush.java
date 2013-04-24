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

package com.yahoo.omid;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;

public class TestAutoFlush extends OmidTestBase {

    @Test
    public void testReadWithSeveralUncommitted() throws Exception {
        byte[] family = Bytes.toBytes(TEST_FAMILY);
        byte[] row = Bytes.toBytes("row");
        byte[] col = Bytes.toBytes("col1");
        byte[] data = Bytes.toBytes("data");
        TransactionManager tm = new TransactionManager(hbaseConf);
        TTable table = new TTable(hbaseConf, TEST_TABLE);

        // Turn off autoflush
        table.setAutoFlush(false);

        Transaction t = tm.begin();
        Put put = new Put(row);
        put.add(family, col, data);
        table.put(t, put);

        // Data shouldn't be in DB yet
        Get get = new Get(row);
        Result result = table.getHTable().get(get);
        assertEquals("Writes are already in DB", 0, result.size());

        tm.commit(t);

        // After commit data should be there
        result = table.getHTable().get(get);
        assertEquals("Writes were not flushed to DB", 1, result.size());
    }

}
