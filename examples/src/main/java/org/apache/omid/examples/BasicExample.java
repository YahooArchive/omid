/*
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
package org.apache.omid.examples;

import org.apache.omid.transaction.HBaseTransactionManager;
import org.apache.omid.transaction.TTable;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ****************************************************************************************************
 *
 * Example code which demonstrates an atomic write into two different rows in HBase
 *
 * ****************************************************************************************************
 *
 * After building the package with 'mvn clean package' find the resulting examples-{version}-bin.tar.gz file in the
 * 'examples/target' folder. Copy it to the target host and expand with 'tar -zxvf examples-{version}-bin.tar.gz'.
 *
 * Make sure that 'hbase-site.xml' and 'core-site.xml' are either in classpath (see run.sh) or explicitly referenced in
 * configuration file. If a secure HBase deployment is needed, make sure to specify the principal (user) and keytab file.
 *
 * The example requires a user table to perform transactional read/write operations. A table is already specified in
 * the default configuration, and can be created with the following command using the 'hbase shell':
 *
 * <pre>
 * create 'MY_TX_TABLE', {NAME =&gt; 'MY_CF', VERSIONS =&gt; '2147483647', TTL =&gt; '2147483647'}
 * </pre>
 *
 * Make sure that the principal/user has RW permissions for the given table using also the 'hbase shell':
 * <pre>
 * grant '{principal/user}', 'RW', 'MY_TX_TABLE'
 * </pre>
 *
 * Alternatively, a table with a column family already created can be used by specifying the table name and column
 * family identifiers using the command line arguments (see details also in 'run.sh') If a table namespace is required,
 * specify it like this: 'namespace:table_name'
 *
 * Finally, run the example using the 'run.sh' script without arguments or specifying the necessary configuration
 * parameters.
 */
public class BasicExample {

    private static final Logger LOG = LoggerFactory.getLogger(BasicExample.class);

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

        byte[] exampleRow1 = Bytes.toBytes("EXAMPLE_ROW1");
        byte[] exampleRow2 = Bytes.toBytes("EXAMPLE_ROW2");
        byte[] qualifier = Bytes.toBytes("MY_Q");
        byte[] dataValue1 = Bytes.toBytes("val1");
        byte[] dataValue2 = Bytes.toBytes("val2");

        LOG.info("Creating access to Omid Transaction Manager & Transactional Table '{}'", userTableName);
        try (TransactionManager tm = HBaseTransactionManager.newInstance();
             TTable txTable = new TTable(userTableName))
        {
            Transaction tx = tm.begin();
            LOG.info("Transaction {} STARTED", tx);

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
            LOG.info("Transaction {} COMMITTED", tx);
        }

    }

}
