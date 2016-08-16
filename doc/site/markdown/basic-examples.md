# Code Examples

This section guides the developer of Omid-based transactional applications over the different steps required to 
execute multi-row transactions on HBase and the different APIs involved in the process.

## Transactional Contexts and Operations with Omid

Applications requiring transactions will use the interfaces provided by the Omid _Transactional Client_. These interfaces
are provided by the _TransactionManager_ and _TTable_ classes described below.

When starting a transaction, the Omid _Transactional Client_ requests a _start timestamp_ from the _TSO_. This 
timestamp marks the beginning of a transactional context, that is, a transaction. From that point on, the client 
application can perform transactional read and write operations on the data source:

* **Read Ops**. The client first checks if the cell has a _Shadow Cell (SC)_. If the cell has a _SC_ and the 
_commit timestamp_ is lower than the _start timestamp_ of the reading transaction, then the client can "see" the 
cell, that is, it's in its snapshot. If there is no _SC_, the _Commit Table (CT)_ is consulted to find the 
_commit timestamp_ of the cell. If the _commit timestamp_ does not exist in the _CT_, then the cell is assumed to 
below to an aborted transaction.
* **Write Ops**. The client directly writes optimistically in the data source any data cells it wishes adding 
the _start timestamp_ as the version of each data cell. The metadata of each cell written is added also to the 
so-called _write-set_ of the transaction.

To commit a transaction, the client sends the _write-set_ of the transaction to the _TSO_, which will check for 
conflicts in the transaction _write-set_ with other transactions executing concurrently:

* **Conflicts**. The transaction is aborted and the _roll-back_ outcome is returned to the client.
* **No conflicts**. The TSO assigns a _commit timestamp_ to the transaction, writes the mapping 
_start timestamp -> commit timestamp_ to the _CT_ and returns _commit_ as an outcome for the transaction to the client.

On receiving the outcome for the transaction, the client:

* **For roll-backs**, it clears the written cells.
* **For commits**, it updates the _shadow cells_ for the cells in the _write-set_ and clears the entry for that
transaction in the _CT_.

The following sections show step-by-step the process for creating transactions in client applications with the Omid APIs.

## Obtaining the Transaction Manager

In order to use transactions, a client application needs to create an instance of the `TransactionManager` interface
with is part of the Transactional Client described in the architecture. The current Omid version offers an 
implementation of a transaction manager for HBase. Please, make sure `core-site.xml` and `hbase-site.xml` HBase 
configuration files are present in the CLASSPATH of your client application.

To create a transaction manager just add the following code to your application:

```java

    ...
    TransactionManager tm = HBaseTransactionManager.newInstance();
    ...

```

If nothing is specified, the HBase transaction manager instance is created with default configuration settings 
loaded from the `default-hbase-omid-client-config.yml` file included in the HBase Omid client jar. To change the 
client default settings, there are two possibilities:

1) Put the specific configuration settings in a file named `hbase-omid-client-config.yml` and include it in the 
CLASSPATH. The file has the following format:

```

# HBase related
commitTableName: MY_OWN_OMID_COMMIT_TABLE_NAME

# TSO/ZK connection
omidClientConfiguration: !!com.yahoo.omid.tso.client.OmidClientConfiguration
    connectionType: !!com.yahoo.omid.tso.client.OmidClientConfiguration$ConnType ZK
    connectionString: "my_zk_cluster_conn_string"

# Instrumentation
metrics: !!com.yahoo.omid.metrics.NullMetricsProvider [ ]

```

2) Create an instance of the `HBaseOmidClientConfiguration` class in the application code and pass it in the creation
of the transaction manager instance:

```java
    ...    
    HBaseOmidClientConfiguration omidClientConfiguration = new HBaseOmidClientConfiguration();
    omidClientConfiguration.setConnectionType(DIRECT);
    omidClientConfiguration.setConnectionString("my_tso_server_host:54758");
    omidClientConfiguration.setRetryDelayMs(3000);
    
    TransactionManager tm = HBaseTransactionManager.newInstance(omidClientConfiguration);
    ...
```

Please, refer to the [ConfigurationExample](https://github.com/apache/incubator-omid/tree/master/examples/src/main/java/org/apache/omid/examples/ConfigurationExample.java)
in the source code to experiment with the configuration options.

## Creating Transactions

Once the `TransactionManager` is created, client applications can use its interface to demarcate transactional boundaries.

In order to create a transaction the `TransactionManager.begin()` method is used:

```java

    ...
    Transaction tx = tm.begin();
    ...

```

The transaction manager will return an instance of the `Transaction` interface representing the recently created 
transaction. This instance is necessary to instruct the operations on the data source, in which transactional context
they should operate (See next section).

## Executing Transactional Operations

In order to perform transaction operations on data, the client application requires to use a wrapper on the HBase's 
`HTableInterface` abstraction. The wrapper is called `TTable` and is also part of what is described as Transactional
Client in the Omid architecture (See section [About Omid](index.html#What_is_Omid?)). `TTable` basically offers the same interface as
`HTableInterface` enhanced with a parameter representing the transactional context. As was previously described, a
`Transaction` instance is obtained on return of `TransactionManager.begin()` method calls.

```java

    ...
    TTable txTable = new TTable(conf, "EXAMPLE_TABLE"); 
    ...

```

Once the access point to the data has been created, applications can use it to trigger transactional operations:

```java

    private final byte[] family = Bytes.toBytes("EXAMPLE_CF");    
    private final byte[] qualifier = Bytes.toBytes("foo");
    ...
    
    // Retrieve transactionally a specific cell
    Get get = new Get(Bytes.toBytes("EXAMPLE_ROW");
    get.add(family, qualifier);
    Result txGetResult = txTable.get(tx, get);
    ...

    // Add a new cell value inside a transactional context
    Put updatedRow = new Put(Bytes.toBytes("EXAMPLE_ROW");
    updatedRow.add(family, qualifier, Bytes.toBytes("Another_value"));
    txTable.put(tx, updatedRow);
    ...

```

## Committing and Aborting Transactions

Once the client application has finished reading/writting from/into the datasource, it must decide whether to make the
changes visible or to discard them. In order to do this, it must instruct the Omid `TransactionManager` either to
`commit()` or to `rollback()` the transactional context where the changes were produced. In case of commit, the TSO
server will be notified to perform the SI validation phase. If the validation succeeds the changes will be visible to
new transactions started from that point on. Otherwise, it will roll back the changes.

In order to commit the data, the client application should do something like this:

```java

    ...
    try {
        ...
        tm.commit(tx);
    } catch (RollbackException e) {

        // Here the transaction was rolled-back when
        // trying to commit due to conflicts with other
        // some other concurrent transaction
        // 
        // The client application should do whatever is
        // required according to its business logic
        ...
    }

```

A transaction can also be specifically aborted, for example if something goes wrong when executing the business logic:

```java

    ...
    try {
        ...
        if( ! some_business_logic_condition ) {
            tm.rollback(tx);
            throw AnyApplicationException("Changes aborted due to...");
        } 
        tm.commit(tx);
    } catch (RollbackException e) {
        ...
    }

```

## Complete Example

The following example summarizes the steps described above.

```java

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import com.yahoo.omid.transaction.HBaseTransactionManager;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;

public class Example {

    public static final byte[] exampleRow = Bytes.toBytes("EXAMPLE_ROW");
    public static final byte[] family = Bytes.toBytes("EXAMPLE_CF");
    public static final byte[] qualifier = Bytes.toBytes("foo");
    public static final byte[] dataValueToAvoid = Bytes.toBytes("valToAvoid");
    public static final byte[] dataValue = Bytes.toBytes("val");

    public static void main(String[] args) throws Exception {

        try (TransactionManager tm = HBaseTransactionManager.newInstance();
             TTable txTable = new TTable("EXAMPLE_TABLE")) {

            Transaction tx = tm.begin();
            System.out.printl("Transaction started");

            // Retrieve data transactionally
            Get get = new Get(exampleRow);
            get.add(family, qualifier);
            Result txGetResult = txTable.get(tx, get);
            byte[] retrievedValue = txGetResult.getValue(family, qualifier);

            // Add a condition in the application logic to show explicit transaction
            // aborts just for illustrative purposes
            if (Bytes.equals(retrievedValue, dataValueToAvoid)) {
                tm.rollback(tx);
                throw new RuntimeException("Illegal value found in database!");
            }

            // Otherwise, add a value in other column and try to commit the transaction
            try {
                Put putOnRow = new Put(exampleRow);
                putOnRow.add(family, qualifier, dataValue);
        	    txTable.put(tx, putOnRow);
                tm.commit(tx);
                System.out.println("Transaction committed. New value written to example row");
            } catch(RollbackException e) {
                System.out.println("Transaction aborted due to conflicts. Changes to row aborted");
            }

        }

    }

}

```

## Additional Examples

The `examples` module contains [complete examples](https://github.com/apache/incubator-omid/tree/master/examples/src/main/java/org/apache/omid/examples)
showing the Omid functionality that can be executed in your Omid+HBase environment. Just clone the Omid project, go
to the `examples` module and execute `mvn clean package` to create a tar.gz file that includes all the examples. 
In order to execute each example, just execute the `run.sh` script and follow the instructions.