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
package com.yahoo.omid.transaction;

import java.io.IOException;

import com.google.common.base.Optional;

/**
 * An behavior that needs to be implemented by transaction managers 
 * to try to locate the possible commit timestamp for a cell. It
 * allows to the transaction managers implementing this interface to
 * search the commit timestamp in a local cache of their own or in 
 * their particular shadow cells implementation.
 */
public interface CommitTimestampLocator {

    public Optional<Long> readCommitTimestampFromCache(long startTimestamp);
    public Optional<Long> readCommitTimestampFromShadowCell(long startTimestamp)
            throws IOException;

}
