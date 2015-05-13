/**
 * Copyright 2011-2015 Yahoo Inc.
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
package com.yahoo.omid.committable;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

public interface CommitTable {

    ListenableFuture<Writer> getWriter();
    ListenableFuture<Client> getClient();

    public interface Writer extends Closeable {
        void addCommittedTransaction(long startTimestamp, long commitTimestamp) throws IOException;
        void updateLowWatermark(long lowWatermark) throws IOException;
        // TODO Make this synchronous
        ListenableFuture<Void> flush();
    }

    public interface Client extends Closeable {
        ListenableFuture<Optional<Long>> getCommitTimestamp(long startTimestamp);
        ListenableFuture<Long> readLowWatermark();
        ListenableFuture<Void> completeTransaction(long startTimestamp);
    }
}
