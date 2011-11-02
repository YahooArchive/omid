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
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.yahoo.omid.client.CommitUnsuccessfulException;
import com.yahoo.omid.client.TransactionManager;
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.client.TransactionalTable;

public class TestTransactionConflict extends OmidTestBase {
	private static final Log LOG = LogFactory
			.getLog(TestTransactionConflict.class);

	private static final String TEST_TABLE = "test";
	private static final String TEST_FAMILY = "data";

	@Before
	public void setUp() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);

		if (!admin.tableExists(TEST_TABLE)) {
			HTableDescriptor desc = new HTableDescriptor(TEST_TABLE);
			HColumnDescriptor datafam = new HColumnDescriptor(TEST_FAMILY);
			datafam.setMaxVersions(10);
			desc.addFamily(datafam);

			admin.createTable(desc);
		}

		admin.enableTable(TEST_TABLE);
		HTableDescriptor[] tables = admin.listTables();
		for (HTableDescriptor t : tables) {
			LOG.info(t.getNameAsString());
		}
	}

	@After
	public void tearDown() {
		try {
			LOG.info("tearing Down");
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(TEST_TABLE);
			admin.deleteTable(TEST_TABLE);

		} catch (Exception e) {
			LOG.error("Error tearing down", e);
		}
	}

    @Test
    public void runTestWriteWriteConflict() throws Exception {
        TransactionManager tm = new TransactionManager(conf);
        TransactionalTable tt = new TransactionalTable(conf, TEST_TABLE);

        TransactionState t1 = tm.beginTransaction();
        LOG.info("Transaction created " + t1);

        TransactionState t2 = tm.beginTransaction();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.add(fam, col, data1);
        tt.put(t1, p);

        Put p2 = new Put(row);
        p2.add(fam, col, data2);
        tt.put(t2, p2);

        tm.tryCommit(t2);

        boolean aborted = false;
        try {
            tm.tryCommit(t1);
            assertTrue("Transaction commited successfully", false);
        } catch (CommitUnsuccessfulException e) {
            aborted = true;
        }
        assertTrue("Transaction didn't raise exception", aborted);
    }

    @Test
    public void runTestCleanupAfterConflict() throws Exception {
        TransactionManager tm = new TransactionManager(conf);
        TransactionalTable tt = new TransactionalTable(conf, TEST_TABLE);

        TransactionState t1 = tm.beginTransaction();
        LOG.info("Transaction created " + t1);

        TransactionState t2 = tm.beginTransaction();
        LOG.info("Transaction created" + t2);

        byte[] row = Bytes.toBytes("test-simple");
        byte[] fam = Bytes.toBytes(TEST_FAMILY);
        byte[] col = Bytes.toBytes("testdata");
        byte[] data1 = Bytes.toBytes("testWrite-1");
        byte[] data2 = Bytes.toBytes("testWrite-2");

        Put p = new Put(row);
        p.add(fam, col, data1);
        tt.put(t1, p);

        Get g = new Get(row).setMaxVersions();
        Result r = tt.get(g);
        assertEquals("Unexpected size for read.", 1, r.size());
        assertTrue(
                "Unexpected value for read: "
                + Bytes.toString(r.getValue(fam, col)),
                Bytes.equals(data1, r.getValue(fam, col)));

        Put p2 = new Put(row);
        p2.add(fam, col, data2);
        tt.put(t2, p2);
        
        r = tt.get(g);
        assertEquals("Unexpected size for read.", 2, r.size());

        tm.tryCommit(t2);

        boolean aborted = false;
        try {
            tm.tryCommit(t1);
            assertTrue("Transaction commited successfully", false);
        } catch (CommitUnsuccessfulException e) {
            aborted = true;
        }
        assertTrue("Transaction didn't raise exception", aborted);
        
        r = tt.get(g);
        assertEquals("Unexpected size for read.", 1, r.size());
        assertTrue(
                "Unexpected value for read: "
                + Bytes.toString(r.getValue(fam, col)),
                Bytes.equals(data2, r.getValue(fam, col)));
    }


    @Test public void testCleanupWithDeleteRow() throws Exception {
       try {
          TransactionManager tm = new TransactionManager(conf);
          TransactionalTable tt = new TransactionalTable(conf, TEST_TABLE);
          
          TransactionState t1 = tm.beginTransaction();
          LOG.info("Transaction created " + t1);
          
          int rowcount = 10;
          int count = 0;

          byte[] fam = Bytes.toBytes(TEST_FAMILY);
          byte[] col = Bytes.toBytes("testdata");
          byte[] data1 = Bytes.toBytes("testWrite-1");
          byte[] data2 = Bytes.toBytes("testWrite-2");
          
          byte[] modrow = Bytes.toBytes("test-del" + 3);
          for (int i = 0; i < rowcount; i++) {
             byte[] row = Bytes.toBytes("test-del" + i);
             
             Put p = new Put(row);
             p.add(fam, col, data1);
             tt.put(t1, p);
          }
          tm.tryCommit(t1);

          TransactionState t2 = tm.beginTransaction();
          LOG.info("Transaction created " + t2);
          Delete d = new Delete(modrow);
          tt.delete(t2, d);

          ResultScanner rs = tt.getScanner(t2, new Scan());
          Result r = rs.next();
          count = 0;
          while (r != null) {
             count++;
             LOG.trace("row: " + Bytes.toString(r.getRow()) + " count: " + count);
             r = rs.next();
          }
          assertEquals("Wrong count", rowcount - 1, count);
          
          TransactionState t3 = tm.beginTransaction();
          LOG.info("Transaction created " + t3);
          Put p = new Put(modrow);
          p.add(fam, col, data2);
          tt.put(t3, p);
          
          tm.tryCommit(t3);

          boolean aborted = false;
          try {
              tm.tryCommit(t2);
              assertTrue("Didn't abort", false);
          } catch (CommitUnsuccessfulException e) {
              aborted = true;
          }
          assertTrue("Didn't raise exception", aborted);
          
          TransactionState tscan = tm.beginTransaction();
          rs = tt.getScanner(tscan, new Scan());
          r = rs.next();
          count = 0;
          while (r != null) {
             count++;            
             r = rs.next();
          }
          assertEquals("Wrong count", rowcount, count);

       } catch (Exception e) {
          LOG.error("Exception occurred", e);
          throw e;
       }
    }
}