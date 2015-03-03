/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.transaction;

import com.yahoo.omid.tsoclient.CellId;

public interface PostCommitActions {

    /**
     * Allows specific implementations to update the shadow cells.
     * @param transaction
     *            the transaction to update shadow cells for
     * @throws TransactionManagerException
     */
    void updateShadowCells(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException;

    /**
     * Allows specific implementations to remove the transaction entry from the commit table.
     * @param transaction
     *            the transaction to remove the commit table entry
     * @throws TransactionManagerException
     */
    void removeCommitTableEntry(AbstractTransaction<? extends CellId> transaction) throws TransactionManagerException;

    /**
     * Allows specific implementations to update the shadow cells and remove the transaction entry from the commit
     * table sequentially.
     * @param transaction
     *            the transaction to update the shadow cells and remove the commit table entry
     * @throws TransactionManagerException
     */
    void updateShadowCellsAndRemoveCommitTableEntry(AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException;

}
