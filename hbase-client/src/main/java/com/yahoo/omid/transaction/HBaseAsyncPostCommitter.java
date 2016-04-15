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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.yahoo.omid.tso.client.CellId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class HBaseAsyncPostCommitter implements PostCommitActions {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseAsyncPostCommitter.class);

    private PostCommitActions syncPostCommitter;

    private ListeningExecutorService postCommitExecutor;

    public HBaseAsyncPostCommitter(PostCommitActions postCommitter, ListeningExecutorService  postCommitExecutor) {
        this.syncPostCommitter = postCommitter;
        this.postCommitExecutor = postCommitExecutor;
    }

    @Override
    public ListenableFuture<Void> updateShadowCells(final AbstractTransaction<? extends CellId> transaction) {

        return postCommitExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                syncPostCommitter.updateShadowCells(transaction);
                return null;
            }

        });

    }

    @Override
    public ListenableFuture<Void> removeCommitTableEntry(final AbstractTransaction<? extends CellId> transaction) {

        return postCommitExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                syncPostCommitter.removeCommitTableEntry(transaction);
                return null;
            }
        });
    }

}
