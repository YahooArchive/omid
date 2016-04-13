# Basic Transaction Algorithm

A simplified version of the algorithm for performing transactional operations in a non-faulty scenario is depicted 
in the following figure:

![Basic algorithm for managing transactions](images/basic-alg.png)

Initially, a user application (not shown in the figure for the sake of simplicity) starts a new transaction using 
the Transactional Client API, which in turn acquires a start timestamp from the TSO.

Then, the user application logic, also through the Transactional Client API, performs a series of read and write 
operations in the snapshot provided by the transactional context recently acquired.

In the case of reads, if the cells read have shadow cells and their value is in the snapshot for the transaction, 
their value is taken directly. If the commit timestamp is missed in the shadow cell, the Transactional Client will 
try to find it in the commit table. If the commit timestamp is found in there and the cell value is in the snapshot, 
the cell value is taken. However, if the commit timestamp is missed, the shadow cell is re-checked. If it still does 
not exist, the cell is ignored and another version that matches the snapshot will be retrieved from the datastore.

When the application commits the transaction, the Transactional Client contacts the TSO in order to check the possible 
conflicts of the transaction. If the writeset does not conflict with other concurrent transactions, the transaction is
committed, the TSO updates the commit table and replies back to the Transactional Client, which in turn, returns the 
control to the user application.

Finally, the Transactional Client, on receiving the commit acknowledgement, updates the shadow cells with the required 
data. After this, it can also remove safely the entry added by the TSO in the commit table.

In case the TSO detects conflicts, the transaction is aborted (not shown in the figure), the Transactional Client will 
clean up the data written to the datastore, and will inform the user application throwing a rollback exception.