/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.examples.hbase;

import com.yahoo.omid.transaction.HBaseTransactionManager;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;
import com.yahoo.omid.tsoclient.TSOClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class OmidBasicExample {

    public static void main(String[] args) throws Exception {

        final byte[] exampleRow1 = Bytes.toBytes("EXAMPLE_ROW1");
        final byte[] exampleRow2 = Bytes.toBytes("EXAMPLE_ROW2");
        final byte[] family = Bytes.toBytes("MY_CF");
        final byte[] qualifier = Bytes.toBytes("MY_Q");
        final byte[] dataValue1 = Bytes.toBytes("val1");
        final byte[] dataValue2 = Bytes.toBytes("val2");

        Configuration conf = HBaseConfiguration.create();
        conf.set(TSOClient.TSO_HOST_CONFKEY, TSOClient.DEFAULT_TSO_HOST);
        conf.setInt(TSOClient.TSO_PORT_CONFKEY, TSOClient.DEFAULT_TSO_PORT);

        TransactionManager tm = HBaseTransactionManager.newBuilder()
                .withConfiguration(conf)
                .build();

        try (TTable tt = new TTable(conf, "MY_TX_TABLE")) {

            Transaction tx = tm.begin();
            Put row1 = new Put(exampleRow1);
            row1.add(family, qualifier, dataValue1);
            tt.put(tx, row1);
            Put row2 = new Put(exampleRow2);
            row2.add(family, qualifier, dataValue2);
            tt.put(tx, row2);
            tm.commit(tx);
        }

        tm.close();
    }

}
