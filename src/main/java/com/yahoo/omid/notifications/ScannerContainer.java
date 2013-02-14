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
package com.yahoo.omid.notifications;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import scala.actors.threadpool.Arrays;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ScannerContainer {

    private static final Logger logger = Logger.getLogger(ScannerContainer.class);

    private static final long TIMEOUT = 3;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    private final ExecutorService exec;
    private Configuration config = HBaseConfiguration.create();

    private Interest interest;
    private AppSandbox appSandbox;


    /**
     * @param interest
     * @param appSandbox
     * @throws IOException 
     */
    public ScannerContainer(String interest, AppSandbox appSandbox) throws IOException {
        this.interest = Interest.fromString(interest);
        this.appSandbox = appSandbox;
        this.exec = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Scanner container [" + interest + "]").build());

        // Generate scaffolding on HBase to maintain the information required to
        // perform notifications
        HBaseAdmin admin = new HBaseAdmin(config);
        try { // TODO: This code should not be here in a production system
              // because it disables the table to add a CF
            HTableDescriptor tableDesc = admin.getTableDescriptor(this.interest.getTableAsHBaseByteArray());
            if (!tableDesc.hasFamily(Bytes.toBytes(Constants.HBASE_META_CF))) {
                String tableName = this.interest.getTable();

                admin.disableTable(tableName);

                HColumnDescriptor metaCF = new HColumnDescriptor(Constants.HBASE_META_CF);
                admin.addColumn(tableName, metaCF); // CF for storing metadata
                                                    // related to the notif.
                                                    // framework

                // TODO I think that coprocessors can not be added dynamically.
                // It has been moved to OmidInfrastructure
                // Map<String, String> params = new HashMap<String, String>();
                // tableDesc.addCoprocessor(TransactionCommittedRegionObserver.class.getName(),
                // null, Coprocessor.PRIORITY_USER, params);
                admin.enableTable(tableName);
                logger.trace("Column family metadata added!!!");
            } else {
                logger.trace("Column family metadata was already added!!! Skipping...");
            }            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            admin.close();
        }
    }

    public void start() throws Exception {
        // TODO Start the number of required scanners instead of only one
        exec.execute(new Scanner());
        logger.trace("Scanners on " + interest + " started");
    }

    public void stop() throws InterruptedException {
        exec.shutdownNow();
        exec.awaitTermination(TIMEOUT, UNIT);
        logger.trace("Scanners on " + interest + " stopped");
    }

    private class Scanner implements Runnable {

        private HTable table = null;
        private Random regionRoller = new Random();
        private Scan scan = new Scan();

        @Override
        public void run() { // Scan and notify
            ResultScanner scanner = null;
            configureBasicScanProperties();
            try {
                table = new HTable(config, interest.getTable());
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        logger.trace("Scanner on " + interest + " is waiting 5 seconds between scans");
                        Thread.sleep(5000);
                        chooseRandomRegionToScan();
                        scanner = table.getScanner(scan);
                        for (Result result : scanner) { // TODO Maybe paginate the result traversal
                            for (KeyValue kv : result.raw()) {
                                if(!Arrays.equals(kv.getFamily(), Bytes.toBytes(Constants.HBASE_META_CF))) {
                                    UpdatedInterestMsg msg = new UpdatedInterestMsg(interest.toStringRepresentation(), kv.getRow());
                                    appSandbox.getAppInstanceRedirector().tell(msg);
                                }
                            }
                        }
                    } catch (IOException e) {
                        logger.warn("Can't get scanner for table " + interest.getTable() + " retrying");
                    }
                }
            } catch (InterruptedException e) {
                logger.warn("Scanner on interest " + interest + " finished");          
            } catch (IOException e) {
                logger.warn("Scanner on interest " + interest + " not initiated because can't get table");
            } finally {
                if(scanner != null) {
                    scanner.close();
                }
                if(table != null) {
                    try {
                        table.close();
                    } catch (IOException e) {
                        // Ignore
                    }
                }
            }
        }

        private void configureBasicScanProperties() {
            byte[] cf = Bytes.toBytes(Constants.HBASE_META_CF);
            // Pattern for observer column in framework's metadata column
            // family: <cf>/<c>-notify
            String column = interest.getColumnFamily() + "/" + interest.getColumn() + Constants.HBASE_NOTIFY_SUFFIX;
            byte[] c = Bytes.toBytes(column);
            byte[] v = Bytes.toBytes("true");
            
            // Filter by value of the notify column
            SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(cf, c, CompareFilter.CompareOp.EQUAL, new BinaryComparator(v));
            valueFilter.setFilterIfMissing(true);
            scan.setFilter(valueFilter);
        }
        
        private void chooseRandomRegionToScan() {
            try {
                Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
                byte[][] startKeys = startEndKeys.getFirst();
                byte[][] endKeys = startEndKeys.getSecond();
                int regionIdx = regionRoller.nextInt(startKeys.length);
                
                scan.setStartRow(startKeys[regionIdx]);
                byte[] stopRow = endKeys[regionIdx];
                // Take into account that the stop row is exclusive, so we need
                // to pad a trailing 0 byte at the end
                if (stopRow.length != 0) { // This is to avoid add the trailing
                    // 0 if there's no stopRow specified
                    stopRow = addTrailingZeroToBA(endKeys[regionIdx]);
                }
                scan.setStopRow(stopRow);
                logger.trace("Number of startKeys and endKeys in table: " + startKeys.length + " " + endKeys.length);
                logger.trace("Region chosen: " + regionIdx);
                logger.trace("Start & Stop Keys: " + Arrays.toString(startKeys[regionIdx]) + " "
                        + Arrays.toString(stopRow));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        // Returns a new bytearray that will occur after the original one passed
        // by padding its contents with a trailing 0 byte (remember that a
        // byte[] is initialized to 0)
        private byte[] addTrailingZeroToBA(byte[] originalBA) {
            byte[] resultingBA = new byte[originalBA.length + 1];
            System.arraycopy(originalBA, 0, resultingBA, 0, originalBA.length);
            return resultingBA;
        }
    }
}
