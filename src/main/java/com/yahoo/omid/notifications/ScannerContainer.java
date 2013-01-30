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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class ScannerContainer {

    private static final Logger logger = Logger.getLogger(ScannerContainer.class);

    private static final long TIMEOUT = 3;
    private static final TimeUnit UNIT = TimeUnit.SECONDS;

    private final ExecutorService exec = Executors.newSingleThreadExecutor();

    private String interest;
    private AppSandbox appSandbox;
    private HTable table;
    
    private final List<String> interestedApps = new CopyOnWriteArrayList<String>();

    /**
     * @param interest
     * @param appSandbox 
     */
    public ScannerContainer(String interest, AppSandbox appSandbox) {
        this.interest = interest;
        this.appSandbox = appSandbox;

        // Generate scaffolding on HBase to maintain the information required to perform notifications
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin admin;
        try { // TODO: This code should not be here in a production system because it disables the table to add a CF
            Interest interestRep = Interest.fromString(this.interest);
            admin = new HBaseAdmin(config);
            HTableDescriptor tableDesc = admin.getTableDescriptor(interestRep.getTableAsHBaseByteArray());
            String metaCFName = Constants.HBASE_META_CF;
            if(!tableDesc.hasFamily(Bytes.toBytes(metaCFName))) {                
                String tableName = interestRep.getTable();
        
                admin.disableTable(tableName);
        
                HColumnDescriptor metaCF = new HColumnDescriptor(metaCFName);
                admin.addColumn(tableName, metaCF); // CF for storing metadata related to the notif. framework
                        
                // TODO I think that coprocessors can not be added dynamically. It has been moved to OmidInfrastructure
                // Map<String, String> params = new HashMap<String, String>();
                //tableDesc.addCoprocessor(TransactionCommittedRegionObserver.class.getName(), null, Coprocessor.PRIORITY_USER, params);
                admin.enableTable(tableName);
                logger.trace("Column family metadata added!!!");
            } else {
                logger.trace("Column family metadata was already added!!! Skipping...");
            }
            table = new HTable(config,interestRep.getTable());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start()
            throws Exception {
        // TODO Start the number of required scanners instead of only one
        exec.execute(new Scanner());
        logger.trace("Scanners on " + interest + " started");
    }

    public void stop() throws InterruptedException {
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        exec.shutdownNow();
        exec.awaitTermination(TIMEOUT, UNIT);
        logger.trace("Scanners on " + interest + " stopped");
    }

    private class Scanner implements Runnable {

        private Scan scan;

        public Scanner() {
            // TODO Connect to HBase in order to perform periodic scans of a particular column value
            Interest schema = Interest.fromString(interest);
            String columnFamily = Constants.HBASE_META_CF;
            byte[] cf = Bytes.toBytes(columnFamily);
            // Pattern for observer column in framework's metadata column family: <cf>/<c>:notify 
            String column = schema.getColumnFamily() + "/" + schema.getColumn() + Constants.HBASE_NOTIFY_SUFFIX;
            byte[] c = Bytes.toBytes(column);
            byte[] v = Bytes.toBytes("true");
            // Filter by CF and by value of the notify column
            Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(cf));
            Filter valueFilter = new SingleColumnValueFilter(cf, c, CompareFilter.CompareOp.EQUAL, new BinaryComparator(v));

            FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
            filterList.addFilter(familyFilter);
            filterList.addFilter(valueFilter);
            
            scan = new Scan();
            scan.setFilter(filterList);
        }

        @Override
        public void run() { // Scan and notify
            ResultScanner scanner;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    try {

                        logger.trace("Scanner on " + interest + " is waiting 5 seconds between scans");
                        Thread.sleep(5000);

                        scanner = table.getScanner(scan);
                        for (Result result : scanner) {
                            for (KeyValue kv : result.raw()) {
                                UpdatedInterestMsg msg = new UpdatedInterestMsg(interest, kv.getRow());
                                appSandbox.getAppInstanceRedirector().tell(msg);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } catch (InterruptedException e) {
                logger.trace("Scanner on interest " + interest + " finished");
            }
        }
    }
}
