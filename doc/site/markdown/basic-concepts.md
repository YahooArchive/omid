# Basic Concepts

Omid started as a research project in 2011 at Yahoo Labs, with the aim of adding transactions to HBase.
While Omid was designed with HBase in mind, we have been careful to not bind ourselves to a particular big 
datastore. As a consequence, Omid can be potentially adapted to be used with any other multiversioned key-value
datastore with some additional extensions.

In the next sections, we introduce two basic concepts that have driven Omid's design.

## Locking vs Lock-Free

When solving the **conflict detection** problem in transactional systems, there are two main approaches that
transaction managers may implement: *locking* and *lock-free*.

With a locking mechanism, the client managing the transaction attempts to acquire a lock on each cell in the writeset
(the set of changes in data performed by the transaction) before committing. If it succeeds in acquiring the lock, and
no other transaction has written to the cells since the start of the client's transaction, it can commit, as it knows 
that no other transaction can write to the cell after the lock has been acquired. Once the commit has completed, the 
client can release the locks on the cells. If the client fails to acquire the lock on any cell or some other transaction
has written to it since the start of the transaction, then the transaction is aborted. 

For example, [Google Percolator](http://research.google.com/pubs/pub36726.html) uses a locking mechanism in its 
transactional system, and so do other subsequent academic and open source transaction managers for NoSQL datastores.

Locking systems can be problematic in a distributed setting and in the presence of process failures. If a client fails
while holding a lock another process must clean up the locks held by that client. However, it is impossible to detect 
reliably if a client has failed or is just being slow. Normally this is dealt with using timeouts, after which the 
client is invalidated and its locks released. However, the timeout must be high enough to ensure that valid clients 
are not invalidated.

In lock-based systems, it is also possible for two transactions to repeatedly conflict with each other over multiple 
retries as there is no defined order in which locks must be acquired.

In a lock-free implementation, this type of livelock is not possible. Transactions can always make progress. A lock-free
implementation has a centralized conflict-detection mechanism. To commit a transaction, each transaction writeset is sent
to this mechanism to check for conflicts with the writesets of other concurrent transactions. If none of those transaction
has written to any of the cells of the writeset since the start of the transaction, the transaction can commit. Otherwise
the transaction is aborted. Omid uses a lock-free mechanism for conflict detection.

## Snapshot Isolation

Isolation in ACID refers to the ability for multiple clients to act on a database without interfering with each other. 
There are various isolation levels, each making a different tradeoff between isolation and concurrent access to the database. 
Omid is only concerned with Snapshot Isolation.

**[Snapshot Isolation (SI)](http://research.microsoft.com/pubs/69541/tr-95-51.pdf)** provides the users of a database with 
a consistent view of the database and the ability to detect write-write conficts between transactions. When a client starts a 
transaction, it is given a *"snapshot view"* of the database. A value read from a cell within this snapshot will always be 
the same, unless the transaction itself updates the cell.

To successfully commit a transaction within snapshot isolation, the system must ensure that no other transaction has written to 
any of the transaction's written cells (that is, its writeset) since the start of the transaction.

That is, transactions with SI guarantees should read from their own data snapshot, being a data snapshot:

* Immutable
* Identified by creation time
* Containing values committed before creation time

In Omid, transactions conflict iff:

* Overlap in time, and...
* ... write to the same cell of a particular row

The following figure describes SI with an example:
![Snapshot Isolation](images/snapshot-isolation.png)

As depicted in the figure, transaction T2 overlaps in time with T1 and T3, but spatially:

* T1 ∩ T2 = ∅
* T2 ∩ T3 = { R4 }

So, transactions T1 and T2, despite overlaping in time, they do not overlap in the elements they modify, so they do not conflict. 
However, transactions T2 and T3 have a conflict because they overlap both in space (both modify R4) and time. Finally, Transaction 
T4 does not has conflicts with other transactions.

Compared to other isolation levels, SI offers good-enough consistency guarantees -although some anomalies may appear on data, e.g. 
write-skew- and performance, mainly due to the fact transactions are not aborted due to read-write conflicts. We took this into 
consideration and we decided to implement SI as the default isolation level for Omid.
