# Management of Client Failures

Upon user application crashes, Transactional Clients may leave some orphaned data in the datastore, even though 
the data in the datastore is kept in a consistent state. When another Transactional Client comes across this data, 
it will check whether the associated transaction was committed or not. If the data belongs to a transaction which 
has not been committed, and the transaction id is lower than the low watermark, then the data is deleted, since it 
belongs to a transaction that can never be committed.

If data belongs to a transaction that was already committed, this means that the Transactional Client crashed between 
sending the commit request and completing the transaction in the commit table. This means that shadow cells have not 
been deleted. The Transactional Client reading  will write the shadow cell for the orphan data. However, it is not able 
to complete the transaction in the commit table as it can not guarantee that shadow cells have been written for all cells 
in the transaction, since only the crashed client knew which cells it had written to. Consequently, this means that the 
commit table can grow indefinitely. Initially, this flaw should not cause many problems, since:

* the amount of data stored is small (16 bytes)
* the number of clients expected to fail between commit and completion is low
* the data is stored on persistent storage. It is not bounded by memory.

For example, if 1% of all Transactional Clients are expected to crash at this exact point in time, while the system is 
writing, on average, 100,000 transactions per second, the amount of data required to store these commit table entries 
for a whole year would be around 500 GB. A single disk could hold this.

In any case, it would be desirable a mechanism to sanitize the Commit Table. If we can guarantee that all orphan data 
has been cleaned-up up to a certain low watermark, then all commit table entries whose start timestamp is lower than 
this low watermark can be deleted.

In the case of HBase, there is already have a compactor which proactively cleans up transactions older than the low 
watermark. This could easily be extended to store the low watermark for each region it compacts, so then another process 
could clean up the commit table based on the minimum low watermark that was stored.