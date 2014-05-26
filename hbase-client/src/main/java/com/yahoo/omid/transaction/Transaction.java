/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.transaction;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

import com.yahoo.omid.client.TSOClient;

/**
 * 
 * This class contains the required information to represent an Omid's transaction, including the set of rows modified.
 * 
 */
public class Transaction {

    public enum Status {
        RUNNING, COMMITTED, ABORTED
    }

    private boolean rollbackOnly;
    private long startTimestamp;
    private long commitTimestamp;
    private Set<HBaseCellIdImpl> cells;
    private Status status = Status.RUNNING;
    private TransactionManager tm;

    Transaction(long startTimestamp, TransactionManager tm) {
        this.cells = new HashSet<HBaseCellIdImpl>();
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = 0;
        this.tm = tm;
    }

    TransactionManager getTransactionManager() {
        return tm;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public String toString() {
        return "Transaction-" + Long.toHexString(startTimestamp);
    }

    public void setRollbackOnly() {
        rollbackOnly = true;
    }

    public boolean isRollbackOnly() {
        return rollbackOnly;
    }

    long getCommitTimestamp() {
        return commitTimestamp;
    }

    void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    Set<HBaseCellIdImpl> getCells() {
        return cells;
    }

    void addCell(HBaseCellIdImpl cell) {
        cells.add(cell);
    }

    Set<HTableInterface> getWrittenTables() {
        Set<HTableInterface> tables = new HashSet<HTableInterface>();
        for (HBaseCellIdImpl cell : cells) {
            tables.add(cell.getTable());
        }
        return tables;
    }

    public Status getStatus() {
        return status;
    }

    void setStatus(Status status) {
        this.status = status;
    }
}
