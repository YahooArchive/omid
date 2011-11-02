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

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.common.io.Files;
import com.yahoo.omid.tso.TSOServer;

public class OmidTestBase {
   private static final Log LOG = LogFactory.getLog(OmidTestBase.class);
   
   private static LocalHBaseCluster hbasecluster;
   protected static Configuration conf;
   private static Thread bkthread;
   private static Thread tsothread;

   @BeforeClass 
   public static void setupOmid() throws Exception {
      bkthread = new Thread() {
            public void run() {
               try {
                  String[] args = new String[1];
                  args[0] = "5";
                  LOG.info("Starting bk");
                  LocalBookKeeper.main(args);
               } catch (InterruptedException e) {
                  // go away quietly
               } catch (Exception e) {
                  LOG.error("Error starting local bk", e);
               }
            }
         };

      tsothread = new Thread("TSO Thread") {
            public void run() {
               try {
                  String[] args = new String[5];
                  args[0] = "1234";
                  args[1] = "100";
                  args[2] = "4";
                  args[3] = "2";
                  args[4] = "localhost:2181";
                  TSOServer.main(args);
               } catch (InterruptedException e) {
                  // go away quietly
               } catch (Exception e) {
                  LOG.error("Error starting TSO", e);
               }
            }
         };
      conf = HBaseConfiguration.create();
      conf.set("tso.host", "localhost");
      conf.setInt("tso.port", 1234);
      final String rootdir = "/tmp/hbase.test.dir/";
      File rootdirFile = new File(rootdir);
      if (rootdirFile.exists()) {
          try {
              Files.deleteRecursively(new File(rootdir));
          } catch (IOException e) {
              LOG.warn("Couldn't delete " + rootdir);
          }
      }
      conf.set("hbase.rootdir", rootdir);

      bkthread.start();

      if (!LocalBookKeeper.waitForServerUp("localhost:2181", 10000)) {
         throw new Exception("Error starting zookeeper/bookkeeper");
      }
      
      Thread.sleep(5000);
      tsothread.start();
      waitForSocketListening("localhost", 1234);
      Thread.sleep(5000);

      hbasecluster = new LocalHBaseCluster(conf, 1, 1);

      hbasecluster.startup();
      
      while (hbasecluster.getActiveMaster() == null || !hbasecluster.getActiveMaster().isInitialized()) {
         Thread.sleep(500);
      }
   }
   
   @AfterClass public static void teardownOmid() throws Exception {
      if (hbasecluster != null) {
         hbasecluster.shutdown();
         hbasecluster.join();
      }

      if (tsothread != null) {
         tsothread.interrupt();
         tsothread.join();
      }

      if (bkthread != null) {
         bkthread.interrupt();
         bkthread.join();
      }
      waitForSocketNotListening("localhost", 1234);
   }

   private static void waitForSocketListening(String host, int port) 
         throws UnknownHostException, IOException, InterruptedException {
      while (true) {
         Socket sock = null;
         try {
            sock = new Socket(host, port);
         } catch (IOException e) {
            // ignore as this is expected
            Thread.sleep(1000);
            continue;
         } finally {
            if (sock != null) {
               sock.close();
            }
         }
         LOG.info(host + ":" + port + " is UP");
         break;
      }   
   }
   
   private static void waitForSocketNotListening(String host, int port) 
         throws UnknownHostException, IOException, InterruptedException {
      while (true) {
         Socket sock = null;
         try {
            sock = new Socket(host, port);
         } catch (IOException e) {
            // ignore as this is expected
            break;
         } finally {
            if (sock != null) {
               sock.close();
            }
         }
         Thread.sleep(1000);
         LOG.info(host + ":" + port + " is still up");
      }   
   }

   protected static void dumpTable(String table) throws Exception {
      HTable t = new HTable(conf, table);
      if (LOG.isTraceEnabled()) {
         ResultScanner rs = t.getScanner(new Scan());
         Result r = rs.next();
         while (r != null) {
            for (KeyValue kv : r.list()) {
               LOG.trace("KV: " + kv.toString() + " value:" + Bytes.toString(kv.getValue()));
            }
            r = rs.next();
         }
      }
   }

   protected static boolean verifyValue(byte[] table, byte[] row, 
                                        byte[] fam, byte[] col, byte[] value) {
      try {
         HTable t = new HTable(conf, table);
         Get g = new Get(row).setMaxVersions(1);
         Result r = t.get(g);
         KeyValue kv = r.getColumnLatest(fam, col);
         
         if (LOG.isTraceEnabled()) {
            LOG.trace("Value for " + Bytes.toString(table) + ":" 
                      + Bytes.toString(row) + ":" + Bytes.toString(fam) 
                      + Bytes.toString(col) + "=>" + Bytes.toString(kv.getValue()) 
                      + " (" + Bytes.toString(value) + " expected)");
         }

         return Bytes.equals(kv.getValue(), value);
      } catch (Exception e) {
         LOG.error("Error reading row " + Bytes.toString(table) + ":" 
                   + Bytes.toString(row) + ":" + Bytes.toString(fam) 
                   + Bytes.toString(col), e);
         return false;
      }
   }
}