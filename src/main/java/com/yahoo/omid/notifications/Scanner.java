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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import scala.actors.threadpool.Arrays;

import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.notifications.thrift.generated.NotificationReceiverService;

public class Scanner implements Runnable {

    private static final Log logger = LogFactory.getLog(Scanner.class);

    private HTable table;
    private String interest;
    private TTransport transport;

    private Random regionRoller = new Random();
    private Scan scan;
    
    private Map<String, List<String>> interestsToObservers;
    private Map<String, List<String>> observersToHosts;

    public Scanner(HTable table, String interest, Map<String, List<String>> interestsToObservers, Map<String, List<String>> observersToHosts) {
        this.table = table;
        this.interest = interest;
        this.interestsToObservers = interestsToObservers;
        this.observersToHosts = observersToHosts;
    }

    @Override
    public void run() { // Scan and notify
        ResultScanner scanner;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {

                    logger.trace("Scanner on " + interest + " is waiting 5 seconds between scans");
                    Thread.sleep(5000);
                    configureScan();
                    scanner = table.getScanner(scan);
                    for (Result result : scanner) {
                        for (KeyValue kv : result.raw()) {
                            if(!Arrays.equals(kv.getFamily(), Bytes.toBytes(Constants.HBASE_META_CF))) {
                                List<String> observers = interestsToObservers.get(interest);
								if (observers != null) {
									for (String observer : observers) {
										List<String> hosts = observersToHosts.get(observer);
										for (String host : hosts) {
											// logger.trace("Notifying " +
											// observer +
											// " on host " + host +
											// " about change on RowKey:" + new
											// String(kv.getRow()) + ", CF: " +
											// new
											// String(kv.getFamily())
											// + " C: " + new
											// String(kv.getQualifier())
											// + " Val: " +
											// Bytes.toString(kv.getValue()));
											notify(observer, kv.getRow(), host, Constants.THRIFT_SERVER_PORT);
										}
									}
								}
                            }
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

    /**
     * 
     */
    private void configureScan() {
        // TODO Connect to HBase in order to perform periodic scans of a particular column value
        Interest schema = Interest.fromString(interest);
        String columnFamily = Constants.HBASE_META_CF;
        byte[] cf = Bytes.toBytes(columnFamily);
        // Pattern for observer column in framework's metadata column family: <cf>/<c>-notify 
        String column = schema.getColumnFamily() + "/" + schema.getColumn() + Constants.HBASE_NOTIFY_SUFFIX;
        byte[] c = Bytes.toBytes(column);
        byte[] v = Bytes.toBytes("true");
        // Filter by value of the notify column
        SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(cf, c, CompareFilter.CompareOp.EQUAL, new BinaryComparator(v));
        valueFilter.setFilterIfMissing(true);
        
        try {
            // Chose a region randomly
            Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
            byte[][] startKeys = startEndKeys.getFirst();
            byte[][] endKeys = startEndKeys.getSecond();
            int regionIdx = regionRoller.nextInt(startKeys.length);
            logger.trace("Number of startKeys and endKeys in table: " + startKeys.length + " " + endKeys.length);
            logger.trace("Region chosen: " + regionIdx);
            scan = new Scan();
            scan.setStartRow(startKeys[regionIdx]);
            byte[] stopRow = endKeys[regionIdx];
            // Take into account that the stop row is exclusive, so we need to pad a trailing 0 byte at the end
            if(stopRow.length != 0) { // This is to avoid add the trailing 0 if there's no stopRow specified
                stopRow = addTrailingZeroToBA(endKeys[regionIdx]);
            }
            logger.trace("Start & Stop Keys: " + Arrays.toString(startKeys[regionIdx]) + " " + Arrays.toString(stopRow));
            scan.setStopRow(stopRow);
            scan.setFilter(valueFilter);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    // Returns a new bytearray that will occur after the original one passed as param
    // by padding its contents with a trailing 0 byte (remember that a byte[] is initialized to 0)
    private byte[] addTrailingZeroToBA(byte[] originalBA){
        byte[] resultingBA = new byte[originalBA.length + 1];
        System.arraycopy(originalBA, 0, resultingBA, 0, originalBA.length);
        return resultingBA;
      }

    private void notify(String observer, byte[] rowKey, String host, int port) {
        // Connect to the notification receiver service (Thrift)
        try {
            transport = new TFramedTransport(new TSocket("localhost", port));
            TProtocol protocol = new TBinaryProtocol(transport);
            NotificationReceiverService.Client client = new NotificationReceiverService.Client(protocol);
            transport.open();
            Interest interest = Interest.fromString(this.interest);
            Notification notification = new Notification(observer, 
                    ByteBuffer.wrap(interest.getTableAsHBaseByteArray()),
                    ByteBuffer.wrap(rowKey), // This is the row that has been modified
                    ByteBuffer.wrap(interest.getColumnFamilyAsHBaseByteArray()),
                    ByteBuffer.wrap(interest.getColumnAsHBaseByteArray()));
            client.notify(notification);
            transport.close();
//            logger.trace("Scanner " + this.interest + " sent this " + notification);
        } catch (Exception e) {
            if (transport != null) {
                transport.close();
            }
            logger.error("Scanner " + interest + "could not sent notification", e);
        } finally {
            if(transport != null) {
                transport.close();
            }
        }
        
    }    

}
