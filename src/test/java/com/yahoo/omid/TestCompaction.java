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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.yahoo.omid.client.TransactionManager;
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.client.TransactionalTable;

public class TestCompaction extends OmidTestBase {
   private static final Log LOG = LogFactory.getLog(TestCompaction.class);

   @Test public void runDeleteOld() throws Exception {
      try {
         TransactionManager tm = new TransactionManager(conf);
         TransactionalTable tt = new TransactionalTable(conf, TEST_TABLE);
         
         TransactionState t1 = tm.beginTransaction();
         LOG.info("Transaction created " + t1);
         
         byte[] row = Bytes.toBytes("test-simple");
         byte[] fam = Bytes.toBytes(TEST_FAMILY);
         byte[] col = Bytes.toBytes("testdata");
         byte[] col2 = Bytes.toBytes("testdata2");
         byte[] data1 = Bytes.toBytes("testWrite-1");
         byte[] data2 = Bytes.toBytes("testWrite-2verylargedatamuchmoredata than anything ever written to");

         Put p = new Put(row);
         p.add(fam, col, data1);
         tt.put(t1, p);
         tm.tryCommit(t1);

         TransactionState t2 = tm.beginTransaction();
         p = new Put(row);
         p.add(fam, col, data2);
         tt.put(t2, p);
         tm.tryCommit(t2);

         for (int i = 0; i < 500; ++i) {
            t2 = tm.beginTransaction();
            p = new Put(row);
            p.add(fam, col2, data2);
            tt.put(t2, p);
            tm.tryCommit(t2);
         }
         
         HBaseAdmin admin = new HBaseAdmin(conf);
         admin.flush(TEST_TABLE);
         
         for (int i = 0; i < 500; ++i) {
            t2 = tm.beginTransaction();
            p = new Put(row);
            p.add(fam, col2, data2);
            tt.put(t2, p);
            tm.tryCommit(t2);
         }

         Get g = new Get(row);
         g.setMaxVersions();
         g.addColumn(fam, col2);
         Result r = tt.get(g);
         int size = r.getColumn(fam, col2).size();
         System.out.println("Size before compaction : " + size);

         admin.compact(TEST_TABLE);
         
         Thread.sleep(2000);
         
         g = new Get(row);
         g.setMaxVersions();
         g.addColumn(fam, col);
         r = tt.get(g);
         assertEquals(1, r.getColumn(fam, col).size());
         
         g = new Get(row);
         g.setMaxVersions();
         g.addColumn(fam, col2);
         r = tt.get(g);
         System.out.println("Size after compaction : " + r.getColumn(fam, col2).size());
         assertThat(r.getColumn(fam, col2).size(), is(lessThan(size)));
      } catch (Exception e) {
         LOG.error("Exception occurred", e);
         throw e;
      }
   }
}