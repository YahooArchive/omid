/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.transaction;

import com.google.common.util.concurrent.ListenableFuture;
import com.yahoo.omid.tsoclient.CellId;

public interface PostCommitActions {

    /**
     * Allows specific implementations to update the shadow cells.
     * @param transaction
     *            the transaction to update shadow cells for
     * @return future signalling end of the computation
     */
    ListenableFuture<Void> updateShadowCells(AbstractTransaction<? extends CellId> transaction);

    /**
     * Allows specific implementations to remove the transaction entry from the commit table.
     * @param transaction
     *            the transaction to remove the commit table entry
     * @return future signalling end of the computation
     */
    ListenableFuture<Void> removeCommitTableEntry(AbstractTransaction<? extends CellId> transaction);

}
