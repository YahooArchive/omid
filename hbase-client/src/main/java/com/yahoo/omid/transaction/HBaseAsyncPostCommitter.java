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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.tsoclient.CellId;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HBaseAsyncPostCommitter implements PostCommitActions {

    private PostCommitActions syncPostCommitExecutor;

    ExecutorService postCommitExecutor =
            Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("postCommit-%d").build());

    public HBaseAsyncPostCommitter(PostCommitActions postCommitExecutor) {
        syncPostCommitExecutor = postCommitExecutor;
    }

    @Override
    public void updateShadowCells(final AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException
    {
        postCommitExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    syncPostCommitExecutor.updateShadowCells(transaction);
                } catch (TransactionManagerException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void removeCommitTableEntry(final AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException
    {
        postCommitExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    syncPostCommitExecutor.removeCommitTableEntry(transaction);
                } catch (TransactionManagerException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void updateShadowCellsAndRemoveCommitTableEntry(final AbstractTransaction<? extends CellId> transaction)
            throws TransactionManagerException
    {
        postCommitExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    syncPostCommitExecutor.updateShadowCells(transaction);
                    syncPostCommitExecutor.removeCommitTableEntry(transaction);
                } catch (TransactionManagerException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
