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

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.omid.transaction.HBaseTransactionManager;
import org.apache.omid.transaction.RollbackException;
import org.apache.omid.transaction.TTable;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * ****************************************************************************************************************
 *
 * Example code which demonstrates the preservation of Snapshot Isolation when writing shared data concurrently
 *
 * ****************************************************************************************************************
 *
 * Please @see{BasicExample} first
 *
 * In the code below, two concurrent transactions (Tx1 and Tx2), try to update the same column in HBase. This will
 * result in the rollback of Tx2 -the last one trying to commit- due to conflicts in the writeset with the previously
 * committed transaction Tx1. Also shows how Tx2 reads the right values from its own snapshot in HBase data.
 *
 * After building the package with 'mvn clean package' find the resulting examples-{version}-bin.tar.gz file in the
 * 'examples/target' folder. Copy it to the target host and expand with 'tar -zxvf examples-{version}-bin.tar.gz'.
 *
 * Make sure that 'hbase-site.xml' and 'core-site.xml' are either in classpath (see run.sh) or explicitly referenced via
 * command line arguments. If a secure HBase deployment is needed, use also command line arguments to specify the
 * principal (user) and keytab file.
 *
 * The example requires a user table to perform transactional read/write operations. A table is already specified in the
 * default configuration, and can be created with the following command using the 'hbase shell':
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
 * parameters if required.
 */
public class SnapshotIsolationExample {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotIsolationExample.class);
    private final byte[] qualifier;
    private final byte[] initialData;
    private final byte[] dataValue1;
    private final byte[] dataValue2;
    private RowIdGenerator rowIdGenerator = new StaticRowIdGenerator();
    private String userTableName;
    private byte[] family;
    private TransactionManager tm;
    private TTable txTable;

    public static void main(String[] args) throws Exception {
        SnapshotIsolationExample example = new SnapshotIsolationExample(args);
        example.execute();
        example.close();
    }

    SnapshotIsolationExample(String[] args) throws IOException, InterruptedException {
        LOG.info("Parsing the command line arguments");
        userTableName = "MY_TX_TABLE";
        if (args != null && args.length > 0 && StringUtils.isNotEmpty(args[0])) {
            userTableName = args[0];
        }
        family = Bytes.toBytes("MY_CF");
        if (args != null && args.length > 1 && StringUtils.isNotEmpty(args[1])) {
            family = Bytes.toBytes(args[1]);
        }
        LOG.info("Table '{}', column family '{}'", userTableName, Bytes.toString(family));

        qualifier = Bytes.toBytes("MY_Q");
        initialData = Bytes.toBytes("initialVal");
        dataValue1 = Bytes.toBytes("val1");
        dataValue2 = Bytes.toBytes("val2");

        LOG.info("--------");
        LOG.info("NOTE: All Transactions in the Example access column {}:{}/{}/{} [TABLE:ROW/CF/Q]",
                 userTableName, Bytes.toString(rowIdGenerator.getRowId()), Bytes.toString(family),
                 Bytes.toString(qualifier));
        LOG.info("--------");

        LOG.info("Creating access to Omid Transaction Manager & Transactional Table '{}'", userTableName);
        tm = HBaseTransactionManager.newInstance();
        txTable = new TTable(userTableName);
    }

    void execute() throws IOException, RollbackException {

        // A transaction Tx0 sets an initial value to a particular column in an specific row
        Transaction tx0 = tm.begin();
        byte[] rowId = rowIdGenerator.getRowId();
        Put initialPut = new Put(rowId);
        initialPut.add(family, qualifier, initialData);
        txTable.put(tx0, initialPut);
        tm.commit(tx0);
        LOG.info("Initial Transaction {} COMMITTED. Base value written in {}:{}/{}/{} = {}",
                 tx0, userTableName, Bytes.toString(rowId), Bytes.toString(family),
                 Bytes.toString(qualifier), Bytes.toString(initialData));

        // Transaction Tx1 starts, creates its own snapshot of the current data in HBase and writes new data
        Transaction tx1 = tm.begin();
        LOG.info("Transaction {} STARTED", tx1);
        Put tx1Put = new Put(rowId);
        tx1Put.add(family, qualifier, dataValue1);
        txTable.put(tx1, tx1Put);
        LOG.info("Transaction {} updates base value in {}:{}/{}/{} = {} in its own Snapshot",
                 tx1, userTableName, Bytes.toString(rowId), Bytes.toString(family),
                 Bytes.toString(qualifier), Bytes.toString(dataValue1));

        // A concurrent transaction Tx2 starts, creates its own snapshot and reads the column value
        Transaction tx2 = tm.begin();
        LOG.info("Concurrent Transaction {} STARTED", tx2);
        Get tx2Get = new Get(rowId);
        tx2Get.addColumn(family, qualifier);
        Result tx2GetResult = txTable.get(tx2, tx2Get);
        Preconditions.checkState(Arrays.equals(tx2GetResult.value(), initialData),
                                 "As Tx1 is not yet committed, Tx2 should read the value set by Tx0 not the value written by Tx1");
        LOG.info(
            "Concurrent Transaction {} should read base value in {}:{}/{}/{} from its Snapshot | Value read = {}",
            tx2, userTableName, Bytes.toString(rowId), Bytes.toString(family),
            Bytes.toString(qualifier), Bytes.toString(tx2GetResult.value()));

        // Transaction Tx1 tries to commit and as there're no conflicting changes, persists the new value in HBase
        tm.commit(tx1);
        LOG.info("Transaction {} COMMITTED. New column value {}:{}/{}/{} = {}",
                 tx1, userTableName, Bytes.toString(rowId), Bytes.toString(family),
                 Bytes.toString(qualifier), Bytes.toString(dataValue1));

        // Tx2 reading again after Tx1 commit must read data from its snapshot...
        tx2Get = new Get(rowId);
        tx2Get.addColumn(family, qualifier);
        tx2GetResult = txTable.get(tx2, tx2Get);
        // ...so it must read the initial value written by Tx0
        LOG.info(
            "Concurrent Transaction {} should read again base value in {}:{}/{}/{} from its Snapshot | Value read = {}",
            tx2, userTableName, Bytes.toString(rowId), Bytes.toString(family),
            Bytes.toString(qualifier), Bytes.toString(tx2GetResult.value()));
        Preconditions.checkState(Arrays.equals(tx2GetResult.value(), initialData),
                                 "Tx2 must read the initial value written by Tx0");

        // Tx2 tries to write the column written by the committed concurrent transaction Tx1...
        Put tx2Put = new Put(rowId);
        tx2Put.add(family, qualifier, dataValue2);
        txTable.put(tx2, tx2Put);
        LOG.info(
            "Concurrent Transaction {} updates {}:{}/{}/{} = {} in its own Snapshot (Will conflict with {} at commit time)",
            tx2, userTableName, Bytes.toString(rowId), Bytes.toString(family),
            Bytes.toString(qualifier), Bytes.toString(dataValue1), tx1);

        // ... and when committing, Tx2 has to abort due to concurrent conflicts with committed transaction Tx1
        try {
            LOG.info("Concurrent Transaction {} TRYING TO COMMIT", tx2);
            tm.commit(tx2);
            // should throw an exception
            Preconditions.checkState(false, "Should have thrown RollbackException");
        } catch (RollbackException e) {
            LOG.info("Concurrent Transaction {} ROLLED-BACK : {}", tx2, e.getMessage());
        }
    }

    private void close() throws IOException {
        tm.close();
        txTable.close();
    }


    void setRowIdGenerator(RowIdGenerator rowIdGenerator) {
        this.rowIdGenerator = rowIdGenerator;
    }

    private class StaticRowIdGenerator implements RowIdGenerator {

        @Override
        public byte[] getRowId() {
            return Bytes.toBytes("EXAMPLE_ROW");
        }
    }
}

