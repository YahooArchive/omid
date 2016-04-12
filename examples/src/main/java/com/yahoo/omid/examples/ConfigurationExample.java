/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.examples;

import com.yahoo.omid.transaction.HBaseOmidClientConfiguration;
import com.yahoo.omid.transaction.HBaseTransactionManager;
import com.yahoo.omid.transaction.RollbackException;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionException;
import com.yahoo.omid.transaction.TransactionManager;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.yahoo.omid.tsoclient.OmidClientConfiguration.ConnType.DIRECT;

/**
 * ****************************************************************************************************************
 *
 *  This example code demonstrates different ways to configure the Omid client settings for HBase
 *
 * ****************************************************************************************************************
 *
 * Please @see{BasicExample} first on how to use with all default settings
 *
 */
public class ConfigurationExample {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationExample.class);

    public static void main(String[] args) throws Exception {

        LOG.info("Parsing command line arguments");
        String userTableName = "MY_TX_TABLE";
        if (args != null && args.length > 0 && StringUtils.isNotEmpty(args[0])) {
            userTableName = args[0];
        }
        byte[] family = Bytes.toBytes("MY_CF");
        if (args != null && args.length > 1 && StringUtils.isNotEmpty(args[1])) {
            family = Bytes.toBytes(args[1]);
        }
        LOG.info("Table '{}', column family '{}'", userTableName, Bytes.toString(family));

        ConfigurationExample example = new ConfigurationExample();

        // -----------------------------------------------------------------------------------------------------------
        // Omid client settings configuration through the 'hbase-omid-client-config.yml' configuration file
        // -----------------------------------------------------------------------------------------------------------
        // The HBaseOmidClientConfiguration loads defaults from 'default-hbase-omid-client-config.yml'
        // and then also applies settings from 'hbase-omid-client-config.yml' if it's available in the classpath.
        // In the code snippet below, the user settings are loaded from the 'hbase-omid-client-config.yml' file in
        // the /conf directory that is included in the example classpath (See run.sh.) You can modify the Omid client
        // settings there or you can place your own 'hbase-omid-client-config.yml' file with all your custom settings
        // in the application classpath.

        example.doWork(userTableName, family, new HBaseOmidClientConfiguration());

        // -----------------------------------------------------------------------------------------------------------
        // Omid client settings configuration from application code
        // -----------------------------------------------------------------------------------------------------------
        // You can also configure Omid programmatically from your code. This is useful for example in unit tests.
        // The HBaseOmidClientConfiguration still loads defaults from 'default-hbase-omid-client-config.yml' first,
        // and then applies settings from 'hbase-omid-client-config.yml' if it's available and then use explicit
        // settings in the code. An example of an explicit Omid client configuration in code is shown below.

        HBaseOmidClientConfiguration omidClientConfiguration = new HBaseOmidClientConfiguration();
        omidClientConfiguration.setConnectionType(DIRECT);
        omidClientConfiguration.setConnectionString("localhost:54758");
        omidClientConfiguration.setRetryDelayInMs(3000);

        example.doWork(userTableName, family, omidClientConfiguration);
    }

    private void doWork(String userTableName, byte[] family, HBaseOmidClientConfiguration configuration)
            throws IOException, TransactionException, RollbackException, InterruptedException {

        byte[] exampleRow1 = Bytes.toBytes("EXAMPLE_ROW1");
        byte[] exampleRow2 = Bytes.toBytes("EXAMPLE_ROW2");
        byte[] qualifier = Bytes.toBytes("MY_Q");
        byte[] dataValue1 = Bytes.toBytes("val1");
        byte[] dataValue2 = Bytes.toBytes("val2");

        LOG.info("Creating access to Omid Transaction Manager & Transactional Table '{}'", userTableName);
        try (TransactionManager tm = HBaseTransactionManager.newInstance(configuration);
             TTable txTable = new TTable(userTableName))
        {
            for (int i = 0; i < 100; i++) {
                Transaction tx = tm.begin();
                LOG.info("Transaction #{} {} STARTED", i, tx);

                Put row1 = new Put(exampleRow1);
                row1.add(family, qualifier, dataValue1);
                txTable.put(tx, row1);
                LOG.info("Transaction {} trying to write a new value in [TABLE:ROW/CF/Q] => {}:{}/{}/{} = {} ",
                         tx, userTableName, Bytes.toString(exampleRow1), Bytes.toString(family),
                         Bytes.toString(qualifier), Bytes.toString(dataValue1));

                Put row2 = new Put(exampleRow2);
                row2.add(family, qualifier, dataValue2);
                txTable.put(tx, row2);
                LOG.info("Transaction {} trying to write a new value in [TABLE:ROW/CF/Q] => {}:{}/{}/{} = {} ",
                         tx, userTableName, Bytes.toString(exampleRow2), Bytes.toString(family),
                         Bytes.toString(qualifier), Bytes.toString(dataValue2));

                tm.commit(tx);
                LOG.info("Transaction #{} {} COMMITTED", i, tx);
            }
        }

    }

}
