/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.yahoo.omid.examples;

import com.yahoo.omid.committable.hbase.CommitTableConstants;
import com.yahoo.omid.tools.hbase.HBaseLogin;
import com.yahoo.omid.transaction.HBaseTransactionManager;
import com.yahoo.omid.transaction.RollbackException;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;
import com.yahoo.omid.tsoclient.TSOClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example executes a simple transaction. After successful build 'mvn clean package' you will find
 * examples-<version>-bin.tar.gz in examples/target folder Copy it to target host and expand with 'tar -zxvf
 * examples-<version>-bin.tar.gz'. Run with run.sh
 *
 * Change table name in the code below to point to an existing table. If you use namespace specify it, ex:
 * namespace:table_name Make sure that hbase-site.xml and core-site.xml are either in classpath (see run.sh) or
 * explicitly referenced via command line arguments. If you have a secure HBase deployemnt use command line arguments to
 * specify principal and keytab file. Make sure that principal has RW permissions for the giving table.
 *
 * Alternatively use the following script to create a new table, ni
 *
 * <pre>
 * create 'sieve_main:example4_test', {NAME => 'cf1', DATA_BLOCK_ENCODING => 'NONE', BLOOMFILTER => 'ROW',
 * REPLICATION_SCOPE => '0',
 * VERSIONS => '2147483647', COMPRESSION => 'LZO', MIN_VERSIONS => '0', TTL => '2147483647', KEEP_DELETED_CELLS =>
 * 'false',
 * BLOCKSIZE => '65536', IN_MEMORY => 'false', BLOCKCACHE => 'true'}
 * grant 'sieve_main', 'RW', 'sieve_main:example4_test'
 * </pre>
 */
public class BasicExample {

    private static final Logger LOG = LoggerFactory.getLogger(BasicExample.class);

    public static void main(String[] args) throws Exception {
        // Change to an existing column family

        LOG.info("Parsing arguments...");
        CLIConfig cliConfig = CLIConfig.parse(args);
        LOG.info("Logging in to Secure HBase");
        HBaseLogin.loginIfNeeded(cliConfig);
        Configuration hbaseConfig = buildConfig(cliConfig);

        LOG.info("Creating a transaction manager...");
        TransactionManager tm = HBaseTransactionManager.newBuilder().withConfiguration(hbaseConfig).build();

        byte[] family = Bytes.toBytes(cliConfig.cfName);
        byte[] exampleRow1 = Bytes.toBytes("EXAMPLE_ROW1");
        byte[] exampleRow2 = Bytes.toBytes("EXAMPLE_ROW2");
        byte[] qualifier = Bytes.toBytes("MY_Q");
        byte[] dataValue1 = Bytes.toBytes("val1");
        byte[] dataValue2 = Bytes.toBytes("val2");

        LOG.info("Running sample transaction table: '{}', cf: '{}'", cliConfig.userTableName, cliConfig.cfName);
        //Change name to an existing table
        try (TTable tt = new TTable(hbaseConfig, cliConfig.userTableName)) {

            Transaction tx = tm.begin();

            Put row1 = new Put(exampleRow1);
            row1.add(family, qualifier, dataValue1);
            tt.put(tx, row1);

            Put row2 = new Put(exampleRow2);
            row2.add(family, qualifier, dataValue2);
            tt.put(tx, row2);

            tm.commit(tx);
            LOG.info("Transaction committed: {}", tx);
        } catch (RollbackException e) {
            LOG.error("Transaction aborted due to conflicts. Changes to row aborted");
        }

        tm.close();
    }

    private static Configuration buildConfig(CLIConfig commandLineConfig) {
        Configuration conf = HBaseConfiguration.create();
        conf.set(TSOClient.TSO_HOST_CONFKEY, commandLineConfig.tsoHost);
        conf.setInt(TSOClient.TSO_PORT_CONFKEY, commandLineConfig.tsoPort);
        conf.setInt(TSOClient.ZK_CONNECTION_TIMEOUT_IN_SECS_CONFKEY, 0);
        conf.setStrings(CommitTableConstants.COMMIT_TABLE_NAME_KEY, commandLineConfig.commitTableName);
        return conf;
    }

}
