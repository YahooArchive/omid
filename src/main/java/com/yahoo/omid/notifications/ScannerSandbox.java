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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
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
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.notifications.AppSandbox.App;
import com.yahoo.omid.notifications.conf.DeltaOmidServerConfig;
import com.yahoo.omid.notifications.metrics.ServerSideInterestMetrics;
import com.yammer.metrics.core.TimerContext;

public class ScannerSandbox {

    private static final Log logger = LogFactory.getLog(ScannerSandbox.class);

    // Lock used by ScannerContainers to protect concurrent accesses to HBaseAdmin when meta-data is created in HTables
    private static final Lock htableLock = new ReentrantLock();

    private DeltaOmidServerConfig conf;

    private Configuration config = HBaseConfiguration.create();

    // Key: Interest where the scanners running on the ScannerContainer will do
    // their work
    // Value: The ScannerContainer that executes the scanner threads scanning
    // each particular interest
    private ConcurrentHashMap<String, ScannerContainer> scanners = new ConcurrentHashMap<String, ScannerContainer>();

    public ScannerSandbox(DeltaOmidServerConfig conf) {
        this.conf = conf;
    }

    public void registerInterestsFromApplication(App app) throws Exception {
        for (String appInterest : app.getInterests()) {
            ScannerContainer scannerContainer = scanners.get(appInterest);
            if (scannerContainer == null) {
                scannerContainer = new ScannerContainer(appInterest);
                ScannerContainer previousScannerContainer = scanners.putIfAbsent(appInterest, scannerContainer);
                if (previousScannerContainer != null) {
                    previousScannerContainer.addInterestedApplication(app);
                    logger.trace("Application added to ScannerContainer for interest " + appInterest);
                    System.out.println("Application added to ScannerContainer for interest " + appInterest);
                    return;
                } else {
                    scannerContainer.start();
                }
            }
            scannerContainer.addInterestedApplication(app);
            logger.trace("ScannerContainer created for interest " + appInterest);
        }
    }

    public void removeInterestsFromApplication(App app) throws InterruptedException {
        for (String appInterest : app.getInterests()) {
            ScannerContainer scannerContainer = scanners.get(appInterest);
            if (scannerContainer != null) {
                synchronized (scannerContainer) {
                    scannerContainer.removeInterestedApplication(app);
                    if (scannerContainer.countInterestedApplications() == 0) {
                        scannerContainer.stop();
                        scanners.remove(appInterest);
                    }
                }
            }
        }
    }

    /**
     * Added for testing
     * 
     * @return a map of scanner containers keyed by interest
     */
    public Map<String, ScannerContainer> getScanners() {
        return scanners;
    }

    public class ScannerContainer {

        private final org.slf4j.Logger logger = LoggerFactory.getLogger(ScannerContainer.class);

        private final long TIMEOUT = 3;
        private final TimeUnit UNIT = TimeUnit.SECONDS;

        private final ExecutorService exec;

        private Interest interest;

        private Set<App> interestedApps = Collections.synchronizedSet(new HashSet<App>());

        ServerSideInterestMetrics metrics;

        /**
         * @param interest
         * @param appSandbox
         * @throws IOException
         */
        public ScannerContainer(String interest) throws IOException {
            this.interest = Interest.fromString(interest);
            metrics = new ServerSideInterestMetrics(interest);

            this.exec = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(
                    "Scanner container [" + interest + "]").build());
            // Generate scaffolding on HBase to maintain the information required to
            // perform notifications
            htableLock.lock();
            HBaseAdmin admin = new HBaseAdmin(config);
            try { // TODO: This code should not be here in a production system
                  // because it disables the table to add a CF
                HTableDescriptor tableDesc = admin.getTableDescriptor(this.interest.getTableAsHBaseByteArray());
                if (!tableDesc.hasFamily(Bytes.toBytes(Constants.HBASE_META_CF))) {
                    String tableName = this.interest.getTable();

                    if (admin.isTableEnabled(tableName)) {
                        admin.disableTable(tableName);
                    }

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
                htableLock.unlock();
            }
        }

        public void addInterestedApplication(App app) {
            interestedApps.add(app);
        }

        public void removeInterestedApplication(App app) {
            interestedApps.remove(app);
        }

        public int countInterestedApplications() {
            return interestedApps.size();
        }

        public void start() throws Exception {
            exec.submit(new Scanner());
            logger.trace("{} scanner(s) on " + interest + " started", "" + 1);
        }

        public void stop() throws InterruptedException {
            exec.shutdownNow();
            exec.awaitTermination(TIMEOUT, UNIT);
            logger.trace("Scanners on " + interest + " stopped");
        }

        public class Scanner implements Callable<Boolean> {

            private HTable table = null;
            private Random regionRoller = new Random();
            private Scan scan = new Scan();

            @Override
            public Boolean call() { // Scan and notify
                long scanIntervalMs = conf.getScanIntervalMs();
                ResultScanner scanner = null;
                try {
                    table = new HTable(config, interest.getTable());
                    long initTimeMillis = System.currentTimeMillis();
                    while (!Thread.currentThread().isInterrupted()) {
                        scan = new Scan();
                        configureBasicScanProperties();

                        try {
                            if (System.currentTimeMillis() < (initTimeMillis + scanIntervalMs)) {
                                long waitTime = scanIntervalMs - (System.currentTimeMillis() - initTimeMillis);
                                logger.trace(interest + " scanner waiting " + waitTime + " millis");
                                Thread.sleep(waitTime);
                            }
                            initTimeMillis = System.currentTimeMillis();
                            chooseRandomRegionToScan();
                            scanner = table.getScanner(scan);
                            logger.trace("NEW scan for {}", interest.toStringRepresentation());
                            try {
                                TimerContext timer = metrics.scanStart();
                                int count = 0;
                                for (Result result : scanner) { // TODO Maybe paginate the result traversal
                                    // TODO check consistent when loading only scanned families?
                                    UpdatedInterestMsg msg = new UpdatedInterestMsg(interest.toStringRepresentation(),
                                            result.getRow());
                                    logger.trace("Found update for {} in row {}", interest.toStringRepresentation(),
                                            Bytes.toString(result.getRow()));
                                    synchronized (interestedApps) {
                                        for (App app : interestedApps) {
                                            app.getAppInstanceRedirector().tell(msg);
                                        }
                                    }
                                    count++;
                                }
                                metrics.scanEnd(timer);
                                metrics.matched(count);
                            } finally {
                                scanner.close();
                            }
                        } catch (IOException e) {
                            logger.warn("Can't get scanner for table " + interest.getTable() + " retrying");
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.warn("Scanner on interest " + interest + " finished");
                } catch (IOException e) {
                    logger.warn("Scanner on interest " + interest + " not initiated because can't get table");
                } finally {
                    if (scanner != null) {
                        scanner.close();
                    }
                    if (table != null) {
                        try {
                            table.close();
                        } catch (IOException e) {
                            // Ignore
                        }
                    }
                }
                logger.error("LEAVING SCANNER");
                return new Boolean(true);
            }

            private void configureBasicScanProperties() {
                byte[] cf = Bytes.toBytes(Constants.HBASE_META_CF);
                // Pattern for observer column in framework's metadata column
                // family: <cf>/<c>-notify
                String column = interest.getColumnFamily() + "/" + interest.getColumn() + Constants.HBASE_NOTIFY_SUFFIX;
                byte[] c = Bytes.toBytes(column);
                byte[] v = Bytes.toBytes("true");

                // Filter by value of the notify column
                SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(cf, c, CompareFilter.CompareOp.EQUAL,
                        new BinaryComparator(v));
                valueFilter.setFilterIfMissing(true);
                scan.setFilter(valueFilter);
                // NOTE fine with respect to consistency: in our case we are only interested in this column
                // scan.setLoadColumnFamiliesOnDemand(true);
                // TODO configurable
                // NOTE: apparently that does not get set from hbase.client.scanner.caching , so we set it manually here
                scan.setCaching(500);
                logger.info("SCANNER WITH CACHING OF " + scan.getCaching());
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

                    logger.trace(
                            "Scanning {} region{} from {} to {}",
                            new String[] { Bytes.toString(table.getTableName()), "" + regionIdx,
                                    Bytes.toString(startKeys[regionIdx]), Bytes.toString(endKeys[regionIdx]) });
                    // logger.trace("Number of startKeys and endKeys in table: " + startKeys.length + " " +
                    // endKeys.length);
                    // logger.trace("Region chosen: " + regionIdx);
                    // logger.trace("Start & Stop Keys: " + Arrays.toString(startKeys[regionIdx]) + " "
                    // + Arrays.toString(stopRow));
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
}
