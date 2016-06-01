/*
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
package org.apache.omid.transaction;

import com.google.common.base.Optional;

/**
 * This interface defines the transaction state and behavior exposed to users.
 */
public interface Transaction {

    enum Status {
        RUNNING, COMMITTED, ROLLEDBACK, COMMITTED_RO
    }

    /**
     * Returns the transaction identifier
     * @return the transaction identifier
     */
    long getTransactionId();

    /**
     * Returns the epoch given by the TSOServer
     * @return the transaction's TSOServer epoch
     */
    long getEpoch();

    /**
     * Returns the current transaction {@link Status}
     * @return transaction status
     */
    Status getStatus();

    /**
     * Forces the transaction to rollback, even when there's an intention
     * to commit it.
     */
    void setRollbackOnly();

    /**
     * Returns whether the transaction was marked for rollback or not
     * @return whether the transaction is marked for rollback or not
     */
    boolean isRollbackOnly();


    /**
     * Set of methods to attach some metadata to a transaction object. One example
     * of such metadata are notifications
     *
     *
     * Expects they metadata stored under key "key" to be of the "Set" type,
     * append "value" to the existing set or creates a new one
     * @param key a key, like in hashtable
     * @param value a value to associate with the given key
     */
    void appendMetadata(String key, Object value);


    void setMetadata(String key, Object value);

    Optional<Object> getMetadata(String key);
}

