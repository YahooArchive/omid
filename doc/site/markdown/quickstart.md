# Quickstart

Below are instructions to quickly set up an environment to test Omid in your local machine.

## Requirements

1. Maven 3.x
2. Java 7
3. HBase 0.98
4. Protobuf 2.5.0

## TSO Setup

### 1. Download and Install the Required HBase Version
You can find HBase distributions in [this page](http://www.apache.org/dyn/closer.cgi/hbase/).
Then start HBase in [standalone mode](https://hbase.apache.org/book.html#quickstart).

### 2. Clone the [Omid repository](https://github.com/francisco-perez-sorrosal/omid) and Build the TSO Package:

```sh
$ git clone git@github.com:yahoo/omid.git
$ cd omid
$ mvn clean install
```
This will generate a binary package containing all dependencies for the TSO in tso-server/target/tso-server-\<VERSION\>-bin.tar.gz. 

**Be aware** Unit tests use HBase mini cluster, it typically fails to start if you are on VPN, thus unit test fail.
Unit tests coverage is also quite extensive and take a while to run on each build (~15min at the moment of writing). So, consider using
`mvn clean install -DskipTests` to speed temporal builds. Note that `-Dmaven.test.skip=true` [is NOT an equivalent](http://ericlefevre.net/wordpress/2008/02/21/skipping-tests-with-maven/).

As an alternative to clone the project, you can download the required version for the TSO tar.gz package from the [release repository](https://bintray.com/yahoo/maven/omid/view).

You can also see the [build history here](https://github.com/yahoo/omid/tags).

### 3. Extract the TSO Package

```sh
$ tar zxvf tso-server-<VERSION>-bin.tar.gz
$ cd tso-server-<VERSION>
```

### 4. Create Omid Tables
Ensure that the setting for hbase.zookeeper.quorum in conf/hbase-site.xml points to your zookeeper instance, and create the 
Timestamp Table and the Commit Table using the omid.sh script included in the bin directory of the tso server:
      
```sh
$ bin/omid.sh create-hbase-commit-table -numRegions 16
$ bin/omid.sh create-hbase-timestamp-table
```

These two tables are required by Omid and they must not be accessed by client applications.

### 5. Start the TSO Server

```sh
$ bin/omid.sh tso
```

This starts the TSO server that in turn will connect to HBase to store information in HBase. By default the TSO listens on 
port 54758. If you want to change the TSO configuration, you can modify the contents in the conf/omid.conf file.

## HBase Client Usage

### 1. Create a New Java Application
Use your favorite IDE an create a new project.

### 2. Add hbase-client Dependency
Choose the right version of the hbase-client jar. For example, in a Maven-based app add the following dependency in the
pom.xml file:

```xml
<dependency>
   <groupId>com.yahoo.omid</groupId>
   <artifactId>hbase-client</artifactId>
   <version>${hbase_client.version}</version>
</dependency>
```

### 3. Start Coding Using the Omid Client Interfaces
In Omid there are two client interfaces: `TTable` and `TransactionManager` (_These interfaces will likely change slightly 
in future._):

1. The `TransactionManager` is used for creating transactional contexts, that is, transactions. A builder is provided in 
the `HBaseTransactionManager` class in order to get the TransactionManager interface.

2. `TTable` is used for putting, getting and scanning entries in a HBase table. TTable's
interface is similar to the standard `HTableInterface`, and only requires passing the transactional context as a
first parameter in the transactional aware methods (e.g. `put(Transaction tx, Put put)`)

## Example Application

Below is provided a sample application accessing data transactionally. Its a dummy application that writes two cells in two 
different rows of a table in a transactional context, but is enough to show how the different Omid client APIs are used. A 
detailed explanation of the client interfaces can be found in the [Basic Examples] section.

```java
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import com.yahoo.omid.transaction.HBaseTransactionManager;
import com.yahoo.omid.transaction.TTable;
import com.yahoo.omid.transaction.Transaction;
import com.yahoo.omid.transaction.TransactionManager;

public class OmidExample {

    public static final byte[] family = Bytes.toBytes("MY_CF");
    public static final byte[] qualifier = Bytes.toBytes("MY_Q");

    public static void main(String[] args) throws Exception {

        try (TransactionManager tm = HBaseTransactionManager.newInstance();
             TTable txTable = new TTable("MY_TX_TABLE")) {

            Transaction tx = tm.begin();

            Put row1 = new Put(Bytes.toBytes("EXAMPLE_ROW1"));
            row1.add(family, qualifier, Bytes.toBytes("val1"));
            txTable.put(tx, row1);

            Put row2 = new Put(Bytes.toBytes("EXAMPLE_ROW2"));
            row2.add(family, qualifier, Bytes.toBytes("val2"));
            txTable.put(tx, row2);

            tm.commit(tx);

        }

    }

}
``` 

To run the application, make sure `core-site.xml` and `hbase-site.xml` for your HBase cluster are present in your 
CLASSPATH. The default configuration settings for the Omid client are loaded from the `default-hbase-omid-client-config.yml` 
file. In order to change the client default settings, you can either; 1) put your specific configuration settings 
in a file named `hbase-omid-client-config.yml` and include it in the CLASSPATH; or 2) do it programmatically in 
the application code by creating an instance of the `HBaseOmidClientConfiguration` class and passing it in the 
creation of the transaction manager:

```java
    import com.yahoo.omid.transaction.HBaseOmidClientConfiguration;

    ...
    
    HBaseOmidClientConfiguration omidClientConfiguration = new HBaseOmidClientConfiguration();
    omidClientConfiguration.setConnectionType(DIRECT);
    omidClientConfiguration.setConnectionString("my_tso_server_host:54758");
    omidClientConfiguration.setRetryDelayMs(3000);
    
    try (TransactionManager tm = HBaseTransactionManager.newInstance(omidClientConfiguration);
             TTable txTable = new TTable("MY_TX_TABLE")) {
    
    ...
```

Also, you will need to create a HBase table "MY_TX_TABLE", with column family "MY_CF", and with `TTL` disabled and
`VERSIONS` set to `Integer.MAX_VALUE`. For example using the HBase shell:

```
create 'MY_TX_TABLE', {NAME => 'MY_CF', VERSIONS => '2147483647', TTL => '2147483647'}
```

This example assumes non-secure communication with HBase. If your HBase cluster is secured with Kerberos, you will need to 
use the `UserGroupInformation` API to log in securely.

## The Omid Compactor Coprocessor

Omid includes a jar with an HBase coprocessor for performing data cleanup that operates during compactions, both minor and
major. Specifically, it does the following:

* Cleans up garbage data from aborted transactions
* Purges deleted cells. Omid deletes work by placing a special tombstone marker in cells. The compactor
  detects these and actually purges data when it is safe to do so (i.e. when there are no committable transactions
  that may read the data).
* 'Heals' committed cells for which the writer failed to write shadow cells.

To deploy the coprocessor, the coprocessor jar must be placed in a location (typically on HDFS) that is accessible
by HBase region servers. The coprocessor may then be enabled on a transactional table by the following steps in the HBase shell:

**1) Disable the table**

```
disable 'MY_TX_TABLE'
```

**2) Add a coprocessor specification to the table via a "coprocessor" attribute. The coprocessor spec may (and usually will)
also include the name of the Omid commit table**

```
alter 'MY_TX_TABLE', METHOD => 'table_att', 'coprocessor'=>'<path_to_omid_coprocessor>/omid-hbase-coprocessor-<coprocessor_version>.jar|com.yahoo.omid.transaction.OmidCompactor|1001|omid.committable.tablename=OMID_COMMIT_TABLE'
```

**3) Add an "OMID_ENABLED => true" flag to any column families which the co-processor should work on**

```
alter 'MY_TX_TABLE', { NAME => 'MY_CF', METADATA =>  {'OMID_ENABLED' => 'true'}}
```

**4) Re-enable the table**

```
enable 'MY_TX_TABLE'
```