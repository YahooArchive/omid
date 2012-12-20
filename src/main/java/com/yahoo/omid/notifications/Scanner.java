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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
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
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.notifications.thrift.generated.NotificationReceiverService;

public class Scanner implements Runnable {

    private static final Log logger = LogFactory.getLog(Scanner.class);

    private HTable table;
    private String interest;
    private TTransport transport;

    private Scan scan;
    
    private Map<String, List<String>> interestsToObservers;
    private Map<String, List<String>> observersToHosts;

    public Scanner(HTable table, String interest, Map<String, List<String>> interestsToObservers, Map<String, List<String>> observersToHosts) {
        this.table = table;
        this.interest = interest;
        this.interestsToObservers = interestsToObservers;
        this.observersToHosts = observersToHosts;
        // TODO Connect to HBase in order to perform periodic scans of a particular column value
        Interest schema = Interest.fromString(interest);
        String columnFamily = schema.getColumnFamily() + Constants.NOTIF_HBASE_CF_SUFFIX;
        byte[] cf = Bytes.toBytes(columnFamily);
        String column = schema.getColumn() + Constants.HBASE_NOTIFY_SUFFIX;
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
        while (true) {
            try {
                try {
                    logger.trace("Scanner on " + interest + " is waiting 5 seconds between scans");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                scanner = table.getScanner(scan);
                for (Result result : scanner) {
                    for(KeyValue kv : result.raw()) {
                        List<String> observers = interestsToObservers.get(interest);
                        for(String observer : observers) {
                            List<String> hosts = observersToHosts.get(observer);
                            for(String host : hosts) {
                                //logger.trace("Notifying " + observer + " on host " + host + " about change on RowKey:" + new String(kv.getRow()) + ", CF: " + new String(kv.getFamily())
                                //+ " C: " + new String(kv.getQualifier()) + " Val: " + Bytes.toString(kv.getValue()));
                                notify(observer, kv.getRow(), host, Constants.THRIFT_SERVER_PORT);
                            }
                        }
                    }                    
                }
            } catch (IOException e) {                
                e.printStackTrace();
            }
        }
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
            //logger.trace("Scanner sent this " + notification + " notification!!!");
        } catch (Exception e) {
            if (transport != null) {
                transport.close();
            }
            e.printStackTrace();
            logger.error("Scanner " + interest + "could not sent notification");
        } finally {
            if(transport != null) {
                transport.close();
            }
        }
        
    }    

}
