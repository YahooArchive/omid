# What is Omid?

**Apache Omid (Optimistically transaction Management In Datastores)** is a flexible, reliable, high performant
and scalable transactional framework that allows Big Data applications to execute ACID transactions on top of 
MVCC key/value NoSQL datastores.

The current implementation provides multi-row transactions on top of Apache HBase, but Omid's design is 
flexible enough to support other datastore implementations as long as they provide MVCC features in their API.

The following sections introduce the motivation behind Omid and its high-level architecture and 
basic concepts. If you want to jump to a more hands-on approach to Omid, please jump to the [Quickstart](quickstart.html) section.
On the other hand, if you want more information about Omid's design and its internals, please refer to the 
sections in the Technical Documentation menuf.

# Why Omid?

A *transaction* comprises a set of data manipulation operations on the state of a database system managed as a single 
unit of work, so all the operations must either entirely be completed (committed) or have no effect (aborted). In other 
words, partial executions of the transaction are not permitted (nor desired in general) because the final state of the 
database can be corrupted.

Without the support for transactions, developers are burdened with ensuring atomic execution of scattered changes in 
data upon failures as well as when there are concurrent accesses to the same data by multiple clients.

In order to process transactions, database systems provide a specific component called the *Transaction Manager*. 
The goal of transaction managers in general is to guarantee the so-called **ACID properties** of transactions: 
*Atomicity*, *Consistency*, *Isolation* and *Durability*. However, ACID properties are hard to scale when databases
have to deal with very large amounts of data and thousands of concurrent users, because the data must be partitioned, 
distributed and replicated. That was the main reason why, with the advent of NoSQL big data stores, transactions were
initially left out of the equation. HBase, Dynamo, BigTable, PNUTS, Cassandra, etc. lacked this precious feature 
initially. However, with the popularization of NoSQL big datastores in many areas of the industry, the need for 
transactions has become a must for certain applications.

Omid fills this gap and provides lock-free transactional support on top of HBase with snapshot isolation guarantees.
Omid enables BigData applications to benefit from the best of both worlds: the scalability provided by NoSQL 
datastores such as HBase, and the concurrency and atomicity provided by transaction processing systems.

## Benefits

The benefits that Omid provides include:

1. Omid allows multi-row/multi-table transactions on top of HBase.
2. Omid is lock-free. In lock-based approaches, the locks on data that are held by the incomplete transactions of 
a failed client prevent others from progressing. In Omid, if a client is slow or faulty, it does not slow down the 
other clients.
3. Omid provides [Snapshot Isolation](basic-concepts.html#Snapshot_Isolation) (SI) guarantees.
4. Omid does not require any modiﬁcation into HBase code. All the transactional logic is implemented in the framework
components.
5. Omid does not require any change into HBase table schema. Omid uses the HBase metadata -the cell timestamp in 
particular- to store the transaction timestamp of each value inserted, updated or deleted in a cell. This enables 
concurrent transactions to read data from the right snapshot.
6. Omid is being used internally at Yahoo in production systems, exhibiting good performance and reliability.

# Architecture Overview

The main architectural components are represented in the figure below and described briefly below in the following
paragraphs.

![Omid's architecture](images/architecture.png)

For a detailed picture of the Omid architecture, please refer to the [Omid Components](omid-components.html) section in the Technical Documentation.

## Component Roles

Omid beneﬁts from a centralized scheme in which a single server, called Transactional Status Oracle (TSO), monitors the
modiﬁed rows/cells by transactions and use that to detect write-write conﬂicts. 

User applications are allowed to begin, commit or abort transactions by means of Transactional Clients (TC), which 
enable remote communication to the TSO and allow to perform transactional operations on the data stored in the datastore.

When a transaction is created, a unique start timestamp is assigned by the Timestamp Oracle (TO). This start timestamp
serves as a transaction identifier and is used by the TSO to guarantee SI by detecting conflicts in the writesset of 
a committing transaction with other concurrent transactions. Upon, finishing conflict detection successfully, the 
TSO assigns the transaction a commit timestamp and writes the mapping start/commit timestamp in the Commit Table (CT)
before returning the response to the client. When receiving the response. the transactional client, adds a Shadow 
Cell (SC) per cell in the transaction writeset in order to allow to resolve the right snapshot for further read 
operations without disturbing the TSO.

The main functionality provided by each component depicted in the figure above can be summarized as follows:

* **Timestamp Oracle (TO)** It assigns the _start_ and _commit timestamps_ that demarcate transactional contexts.
* **Transaction Status Oracle (TSO)**. Its the central element of the Omid architecture on the server-side. Its main
task is to resolve conflicts between concurrent transactions.
* **Commit Table (CT)** It stores a temporary mapping from the start timestamp to the commit timestamp of every 
committed transaction.
* **Transactional Client (TC)** It allows client applications to demarcate transaction boundaries through the so-called
_Transaction Manager_ and to perform transactional read/write operations on data through _Transactional Tables_.
* **Shadow Cells (SC)** These special metadata cells are written alongside data cells in the data store to allow the
transactional clients to resolve read snapshots without consulting the Commit Table. They contain the transactional 
boundaries of the last transaction in writing to the data cell.

For a more in-depth description of how Omid works, please refer to the sections in the Technical Documentation menu.

**Do you want to try Omid now?** Please, go to the [Quickstart](quickstart.html) section.
