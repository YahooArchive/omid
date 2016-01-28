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
package com.yahoo.omid.committable;

import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class NullCommitTable implements CommitTable {
    @Override
    public ListenableFuture<CommitTable.Writer> getWriter() {
        SettableFuture<CommitTable.Writer> f = SettableFuture.<CommitTable.Writer>create();
        f.set(new Writer());
        return f;
    }

    @Override
    public ListenableFuture<CommitTable.Client> getClient() {
        SettableFuture<CommitTable.Client> f = SettableFuture.<CommitTable.Client>create();
        f.set(new Client());
        return f;
    }

    public class Writer implements CommitTable.Writer {
        @Override
        public void addCommittedTransaction(long startTimestamp, long commitTimestamp) {
            // noop
        }

        @Override
        public void updateLowWatermark(long lowWatermark) throws IOException {
            // noop
        }

        @Override
        public void clearWriteBuffer() {
            // noop
        }

        @Override
        public void flush() throws IOException {
            // noop
        }

        @Override
        public void close() {}

    }

    public static class Client implements CommitTable.Client {
        @Override
        public ListenableFuture<Optional<CommitTimestamp>> getCommitTimestamp(long startTimestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<Long> readLowWatermark() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<Void> completeTransaction(long startTimestamp) {
            SettableFuture<Void> f = SettableFuture.<Void>create();
            f.set(null);
            return f;
        }

        @Override
        public ListenableFuture<Boolean> tryInvalidateTransaction(long startTimestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
