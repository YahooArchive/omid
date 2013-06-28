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

import static com.yahoo.omid.notifications.Constants.NOTIFY_TRUE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.notifications.conf.ServerConfiguration;
import com.yahoo.omid.notifications.metrics.ServerSideInterestMetrics;
import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.transaction.RollbackException;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;
import com.yammer.metrics.core.TimerContext;

public class ScannerSandbox {

    private static final Logger logger = LoggerFactory.getLogger(ScannerSandbox.class);

    // Lock used by ScannerContainers to protect concurrent accesses to HBaseAdmin when meta-data is created in HTables
    private static final Lock htableLock = new ReentrantLock();

    private Configuration config = HBaseConfiguration.create();

    private Map<Interest, BlockingQueue<Notification>> handOffQueues = Maps.newHashMap();

    // Key: Interest where the scanners running on the ScannerContainer will do
    // their work
    // Value: The ScannerContainer that executes the scanner threads scanning
    // each particular interest
    private Map<Interest, ScannerContainer> scanners = Maps.newHashMap();

    private final ServerConfiguration serverConfiguration;

    public ScannerSandbox(ServerConfiguration serverConfiguration) {
        this.serverConfiguration = serverConfiguration;
    }

    public synchronized void registerInterestsFromApplication(App app) throws Exception {

        // first initialize handoff queues
        for (Interest appInterest : app.getInterests()) {
            logger.info("Adding handoff queue for {}/{}", app.name, appInterest.toString());
            // XXX use a different size for the queues?
            handOffQueues.put(appInterest,
                    new ArrayBlockingQueue<Notification>(serverConfiguration.getTransferBufferCapacity()));
        }

        logger.info("app interests: " + app.getInterests());
        for (Interest appInterest : app.getInterests()) {
            ScannerContainer scannerContainer = scanners.get(appInterest);
            if (scannerContainer == null) {
                scannerContainer = new ScannerContainer(appInterest);
                if (scanners.containsKey(appInterest)) {
                    logger.error("Cannot add scanners for existing app interest {}", appInterest.toString());
                    continue;
                }
                scannerContainer.start();
                logger.info("ScannerContainer created for interest " + appInterest);
                scanners.put(appInterest, scannerContainer);
            }
            scannerContainer.addInterestedApplication(app);
            logger.info("Application interest {} registered in scanner container for app {}", appInterest, app.name);
        }
    }

    public void removeInterestsFromApplication(App app) throws InterruptedException {
        for (Interest appInterest : app.getInterests()) {
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
    public Map<Interest, ScannerContainer> getScanners() {
        return scanners;
    }

    public BlockingQueue<Notification> getHandoffQueue(Interest interest) {
        return handOffQueues.get(interest);
    }

    public class ScannerContainer {

        private final org.slf4j.Logger logger = LoggerFactory.getLogger(ScannerContainer.class);

        private final long TIMEOUT = 3;
        private final TimeUnit UNIT = TimeUnit.SECONDS;

        private final ExecutorService exec;

        private Interest interest;

        AtomicReference<App> currentApp = new AtomicReference<App>();

        ServerSideInterestMetrics metrics;

        private Future<Boolean> scannerRef;

        private TransactionManager transactionManager;

        /**
         * @param interest
         * @param appSandbox
         * @throws IOException
         */
        public ScannerContainer(Interest interest) throws IOException {
            this.interest = interest;
            metrics = new ServerSideInterestMetrics(interest);

            this.exec = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(
                    "Scanner container [" + interest + "]").build());
            // Generate scaffolding on HBase to maintain the information required to
            // perform notifications
            HBaseAdmin admin = new HBaseAdmin(config);
            logger.info("Starting scanner container");
            htableLock.lock();
            try { // TODO: This code should not be here in a production system
                  // because it disables the table to add a CF
                HTableDescriptor tableDesc = admin.getTableDescriptor(this.interest.getTableAsHBaseByteArray());
                if (!tableDesc.hasFamily(Constants.HBASE_META_CF)) {
                    String tableName = this.interest.getTable();

                    if (admin.isTableEnabled(tableName)) {
                        admin.disableTable(tableName);
                    }

                    HColumnDescriptor metaCF = new HColumnDescriptor(Constants.HBASE_META_CF);
                    admin.addColumn(tableName, metaCF); // CF for storing metadata
                                                        // related to the notif.
                                                        // framework

                    admin.enableTable(tableName);
                    logger.info("Column family metadata added");
                } else {
                    logger.info("Column family metadata was already added, skipping...");
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                admin.close();
                htableLock.unlock();
            }
            transactionManager = new TransactionManager(config);
        }

        public void addInterestedApplication(App app) {
            currentApp.set(app);
        }

        public void removeInterestedApplication(App app) {
            currentApp.set(null);
        }

        public int countInterestedApplications() {
            return (currentApp.get() == null ? 0 : 1);
        }

        public void start() throws Exception {
            scannerRef = exec.submit(new Scanner());
            logger.info("{} scanner(s) on " + interest + " started", "" + 1);
        }

        public void stop() throws InterruptedException {
            scannerRef.cancel(true);
            exec.shutdownNow();
            exec.awaitTermination(TIMEOUT, UNIT);
            logger.info("Scanners on " + interest + " stopped");
        }

        public class Scanner implements Callable<Boolean> {

            private TTable table = null;
            private Random regionRoller = new Random();
            private Scan scan = new Scan();

            public Scanner() {
            }

            @Override
            public Boolean call() { // Scan and notify
                ResultScanner scanner = null;
                try {
                    long scanIntervalMs = serverConfiguration.getScanIntervalMs();
                    table = new TTable(config, interest.getTable());
                    long initTimeMs = System.currentTimeMillis();
                    while (!Thread.currentThread().isInterrupted()) {
                        scan = new Scan();
                        configureBasicScanProperties();

                        // in case of no or very few notifications, scan will be very fast, consuming resources without
                        // actually returning anything. So we pause.
                        if (System.currentTimeMillis() < (initTimeMs + scanIntervalMs)) {
                            long waitTime = scanIntervalMs - (System.currentTimeMillis() - initTimeMs);
                            logger.trace(interest + " scanner waiting " + waitTime + " millis");
                            Thread.sleep(waitTime);
                        }
                        initTimeMs = System.currentTimeMillis();
                        try {
                            chooseRandomRegionToScan();
                            Transaction transaction = transactionManager.begin();

                            try {
                                scanner = table.getScanner(transaction, scan);
                                TimerContext timer = metrics.scanStart();
                                Stopwatch stopwatch = new Stopwatch();
                                int count = 0;
                                for (Result result : scanner) { // TODO Maybe paginate the result traversal
                                    // TODO check consistent when loading only scanned families?
                                    Notification msg = new Notification(ByteBuffer.wrap(result.getRow()));
                                    // logger.trace("Found update for {} in row {}", interest.toStringRepresentation(),
                                    // Bytes.toString(result.getRow()));

                                    if (currentApp.get() == null) {
                                        break;
                                    }

                                    // TODO configurable timeout
                                    stopwatch.start();
                                    if (!getHandoffQueue(interest).offer(msg, 10, TimeUnit.SECONDS)) {
                                        logger.error("Cannot deliver message {} to any receiver application after 10s",
                                                msg);
                                        stopwatch.stop();
                                        break;
                                    }
                                    stopwatch.stop();
                                    count++;
                                }
                                metrics.scanEnd(timer);
                                metrics.matched(count);
                                metrics.offerTime(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                            } catch (IOException e) {
                                logger.warn("Can't get scanner for table " + interest.getTable() + " retrying");
                            } finally {
                                if (scanner != null) {
                                    scanner.close();
                                }
                                try {
                                    transactionManager.commit(transaction);
                                } catch (RollbackException e) {
                                    // ignore, read only transaction
                                }
                            }
                        } catch (RuntimeException e) {
                            logger.error("Unhandled exception while scanning. Will retry scanning", e);
                            // pause before trying again
                            Thread.sleep(serverConfiguration.getScanIntervalMs());
                        }
                    }
                } catch (InterruptedException e) {
                    logger.warn("Scanner on interest " + interest + " finished", e);
                } catch (IOException e) {
                    logger.error("Scanner on interest " + interest + " not initiated because can't get table", e);
                } catch (Exception e) {
                    logger.error("Unhandled error for scanner", e);
                } finally {
                    if (scanner != null) {
                        logger.info("Closing scanner for interest " + interest);
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
                byte[] cf = Constants.HBASE_META_CF;
                // Pattern for observer column in framework's metadata column
                // family: <cf>/<c>-notify
                String column = interest.getColumnFamily() + "/" + interest.getColumn() + Constants.HBASE_NOTIFY_SUFFIX;
                byte[] c = Bytes.toBytes(column);
                byte[] v = NOTIFY_TRUE;

                // Filter by value of the notify column
                SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(cf, c, CompareFilter.CompareOp.EQUAL,
                        new BinaryComparator(v));
                valueFilter.setFilterIfMissing(true);
                scan.setFilter(valueFilter);
                // NOTE fine with respect to consistency: in our case we are only interested in this column
                // scan.setLoadColumnFamiliesOnDemand(true);
                // TODO configurable
                // NOTE: apparently that does not get set from hbase.client.scanner.caching , so we set it manually here
                scan.setCaching(serverConfiguration.getTransferBufferCapacity());
            }

            private void chooseRandomRegionToScan() {
                try {
                    Pair<byte[][], byte[][]> startEndKeys = ((HTable) table.getHTable()).getStartEndKeys();
                    byte[][] startKeys = startEndKeys.getFirst();
                    byte[][] endKeys = startEndKeys.getSecond();
                    if (startKeys.length <= 0) {
                        scan.setStartRow(new byte[0]);
                        scan.setStopRow(new byte[0]);
                        logger.warn("Empty table {} ", table);
                        return;
                    }
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

                    logger.debug(
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
