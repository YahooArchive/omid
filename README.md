Omid
=====

The Omid project provides transactional support for key-value stores using Snapshot Isolation. Omid stands for Optimistically transactional Management in Datasources. At this stage of the project, HBase is the only supported data-store.

If you have any question, please take a look to the [Wiki](https://github.com/yahoo/omid/wiki) or contact us at omid-project@googlegroups.com or read [the online archives](https://groups.google.com/forum/?fromgroups=#!forum/omid-project)

Use Cases
---------

Add UCs.

Basic Architecture
------------------

The main component of Omid is a server called the Status Oracle (TSO.) The TSO contains all the information needed to manage transactions. Applications requiring transactional support in key-value stores need to use the API provided by special components provided by Omid called Transactional Clients (TCs). TCs are in charge of connecting to the TSO and perform the required operations in data-stores. The TSO replicates transactional information to the TCs which just contact the TSO when they want to start a transaction or commit it.

The TSO uses BookKeeper as a Write-Ahead Log where it dumps all its state. In case of crash failures it is possible to restart the TSO without losing any commit information.

The core architecture of the software is described in more detail in the [Technical Details](https://github.com/yahoo/omid/wiki/Technical-Details) section of the Wiki.

Compilation
-----------

Omid uses Maven for its build system.

To compile Omid:

	$ git clone https://github.com/yahoo/omid.git omid
	$ cd omid
    $ mvn install -DskipTests

Tests should run cleanly if you want to run them.

Set-Up
------

To test Omid you might want to run a benchmark.

### Status Oracle
To start the SO, run:
   
    $ bin/omid.sh tso

### Benchmark
To benchmark the TSO alone, run:

    $ bin/omid.sh tsobench

API Description
---------------

The public API is in these classes:

    src/main/java/com/yahoo/omid/transaction/TTable.java
    src/main/java/com/yahoo/omid/transaction/Transaction.java
    src/main/java/com/yahoo/omid/transaction/TransactionManager.java

For an example of usage, take a look to this class:

    src/test/java/com/yahoo/omid/TestBasicTransaction.java

Logging 
-------
The logging preferences can be adjusted in src/main/resources/log4j.properties.

Acknowledgement
-------
This project has been partially supported by the EU Comission through the Cumulo Nimbo project (FP7-257993).