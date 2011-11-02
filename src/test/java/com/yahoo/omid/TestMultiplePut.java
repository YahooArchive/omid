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

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.yahoo.omid.client.TransactionManager;
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.client.TransactionalTable;

public class TestMultiplePut extends OmidTestBase {
   private static final Log LOG = LogFactory.getLog(TestMultiplePut.class);
   
   private static final String TEST_TABLE = "TestMultiplePut";
   private static final String TEST_FAMILY = "Family";

   @Before public void setup() throws Exception {
      HBaseAdmin admin=new HBaseAdmin(conf);
      if (!admin.tableExists(TEST_TABLE)) {
         HTableDescriptor desc=new HTableDescriptor(TEST_TABLE);
         HColumnDescriptor column=new HColumnDescriptor(TEST_FAMILY);
         desc.addFamily(column);

//         HColumnDescriptor delfam = new HColumnDescriptor(TransactionalTable.DELETE_STATUS_FAMILY);
//         delfam.setMaxVersions(10);
//         desc.addFamily(delfam);
         admin.createTable(desc);
         LOG.info("created table");
      }
   }
    
   @After public void tearDown() {
      try {
         HBaseAdmin admin = new HBaseAdmin(conf);
         admin.disableTable(TEST_TABLE);
         admin.deleteTable(TEST_TABLE);
      } catch (Exception e) {
         LOG.error("Error tearing down", e);
      }
   }
    
   @Test public void testMultiPutSameRow() throws Exception {
      try{
         byte[] family = Bytes.toBytes("Family");
         byte[] col1 = Bytes.toBytes("value1");
         byte[] col2 = Bytes.toBytes("value2");
         TransactionManager tm = new TransactionManager(conf);
         TransactionalTable table1 = new TransactionalTable(conf, TEST_TABLE);
         TransactionState t=tm.beginTransaction();
         int val=1000;
         byte[]data=Bytes.toBytes(val);
         Put put1=new Put(data);
         put1.add(family, col1, data);
         table1.put(t,put1);
         Put put2=new Put(data);
         put2.add(family, col2, data);
         table1.put(t,put2);
         tm.tryCommit(t);
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

   @Test public void testManyManyPut() throws Exception {
      try{
         byte[] family = Bytes.toBytes("Family");
         byte[] col = Bytes.toBytes("value");

         TransactionManager tm = new TransactionManager(conf);
         TransactionalTable table1 = new TransactionalTable(conf, TEST_TABLE);
         TransactionState t=tm.beginTransaction();
         int num=50;
         for(int j=0;j<=num;j++) {
            byte[]data=Bytes.toBytes(j);
            Put put=new Put(data);
            put.add(family, col, data);
            table1.put(t,put);
         }
         tm.tryCommit(t);
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