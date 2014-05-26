Omid2
=====

The Omid2 project provides transactional support for key-value stores using Snapshot Isolation. Omid stands for Optimistically transactional Management in Datasources. HBase is the only datastore currently supported, though adaption to any datastore that provides multiple versions per cell should be straightforward.

There are 3 components in OMID2;
 * The Transaction Status Oracle (TSO), which assigns transaction timestamps and resolves conflicts between transactions.
 * The commit table which stores a mapping from start timestamp to commit timestamp
 * Shadow cells, which are written alongside data cells in the datastore to allow client to resolve reads without consulting the commit table.

To start a transaction, the client requests a start timestamp from the TSO. It then writes any data cells it wishes to write (i.e. the write set) with the start timestamp as the version of the data cell. To commit the transaction, the client sends the write set to the TSO, which will check for conflicts. If there is no conflict, the TSO assigns a commit timestamp to the transaction, and writes the mapping of the start timestamp to commit timestamp to the commit table. Once the mapping has been persisted, the commit timestamp is returned to the client. On receiving the commit timestamp from the server, the client updates the shadow cells for the write set and clears the transaction from the commit table.

To read a cell transactionally, the client first checks if the cell has a shadow cell. If the commit timestamp of the shadow cell is lower than the start timestamp of the reading transaction, then the client can "see" the cell. If there is no shadow cell, the commit table is consulted to find the commit timestamp of the cell. If the commit timestamp does not exist in the commit table, then the cell is assumed to below to an aborted transaction.

There are currently two implementations of the commit table, a hbase implementation and an inmemory implementation. The inmemory implementation gives no persistence guarantee and is only useful for benchmarking the TSO. 

Quickstart
----------
Clone the repository and build the TSO package:

      $ git clone git@git.corp.yahoo.com:scalable-computing/omid.git
      $ cd omid
      $ mvn clean install assembly:single

This will generate a binary package containing all dependencies for the TSO in tso-server/target/tso-server-<VERSION>-bin.tar.gz.

Extract this package on your server

      $ tar zxvf tso-server-<VERSION>-bin.tar.gz
      $ cd tso-server-<VERSION>

Ensure that the setting for hbase.zookeeper.quorum in conf/hbase-site.xml points to your zookeeper instance, and create the commit and timestamp tables.
      
      $ bin/omid.sh create-hbase-commit-table -numSplits 16
      $ bin/omid.sh create-hbase-timestamp-table

Then start the TSO.

      $ bin/omid.sh tso

By default the tso listens on port 54758.

Client usage
------------

Add the following to your pom.xml dependencies:
```xml
    <dependency>
      <groupId>com.yahoo.omid</groupId>
      <artifactId>hbase-client</artifactId>
      <version>[VERSION]</version>
    </dependency>
```

The client interfaces for OMID2 are TTable and TransactionManager. TransactionManager is used for creating and committing transactions. TTable can be used for putting, getting and scanning entries in a hbase table. TTable's interface is similar to HTable. _These interfaces will likely change slightly in future._

To run this example, make sure you have hbase-site.xml in your classpath, with hbase.zookeeper.quorum, tso.host and tso.port set accordingly. Also, you will need to create a hbase table "EXAMPLE_TABLE", with column family "EXAMPLE_CF", and with TTL disabled and maxVersions set to Integer.MAX_VALUE.

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;

public class Example {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        TransactionManager tm = TransactionManager.newBuilder()
            .withConfiguration(conf).build();
        TTable tt = new TTable(conf, "EXAMPLE_TABLE");
        byte[] exampleRow1 = Bytes.toBytes("EXAMPLE_ROW1");
        byte[] exampleRow2 = Bytes.toBytes("EXAMPLE_ROW2");
        byte[] family = Bytes.toBytes("EXAMPLE_CF");
        byte[] qualifier = Bytes.toBytes("foo");
        byte[] dataValue1 = Bytes.toBytes("val1");
        byte[] dataValue2 = Bytes.toBytes("val2");

        Transaction tx1 = tm.begin();
        Put row1 = new Put(exampleRow1);
        row1.add(family, qualifier, dataValue1);
        tt.put(tx1, row1);
        Put row2 = new Put(exampleRow2);
        row2.add(family, qualifier, dataValue2);
        tt.put(tx1, row2);

        tm.commit(tx1);
        tt.close();
        tm.close();
    }
}
```