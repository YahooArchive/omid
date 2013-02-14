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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Provides transactional methods for accessing and modifying a given snapshot
 * of data identified by an opaque {@link TransactionState} object.
 * 
 */
public interface TTableInterface extends Closeable {

    /**
     * Gets the name of this table.
     * 
     * @return the table name.
     */
    byte[] getTableName();

    /**
     * Returns the {@link Configuration} object used by this instance.
     * <p>
     * The reference returned is not a copy, so any change made to it will
     * affect this instance.
     */
    Configuration getConfiguration();

    /**
     * Gets the {@link HTableDescriptor table descriptor} for this table.
     * 
     * @throws IOException
     *             if a remote or network exception occurs.
     */
    HTableDescriptor getTableDescriptor() throws IOException;

    /**
     * Test for the existence of columns in the table, as specified in the Get.
     * <p>
     * 
     * This will return true if the Get matches one or more keys, false if not.
     * <p>
     * 
     * This is a server-side call so it prevents any data from being transfered
     * to the client.
     * 
     * @param get
     *            the Get
     * @return true if the specified Get matches one or more keys, false if not
     * @throws IOException
     *             e
     */
    boolean exists(Transaction transaction, Get get) throws IOException;

    // /**
    // * Method that does a batch call on Deletes, Gets and Puts. The ordering
    // of
    // * execution of the actions is not defined. Meaning if you do a Put and a
    // * Get in the same {@link #batch} call, you will not necessarily be
    // * guaranteed that the Get returns what the Put had put.
    // *
    // * @param actions
    // * list of Get, Put, Delete objects
    // * @param results
    // * Empty Object[], same size as actions. Provides access to
    // * partial results, in case an exception is thrown. A null in the
    // * result array means that the call for that action failed, even
    // * after retries
    // * @throws IOException
    // * @since 0.90.0
    // */
    // void batch(Transaction transaction, final List<? extends Row> actions,
    // final Object[] results) throws IOException,
    // InterruptedException;
    //
    // /**
    // * Same as {@link #batch(List, Object[])}, but returns an array of results
    // * instead of using a results parameter reference.
    // *
    // * @param actions
    // * list of Get, Put, Delete objects
    // * @return the results from the actions. A null in the return array means
    // * that the call for that action failed, even after retries
    // * @throws IOException
    // * @since 0.90.0
    // */
    // Object[] batch(Transaction transaction, final List<? extends Row>
    // actions) throws IOException, InterruptedException;
    //
    // /**
    // * Same as {@link #batch(List, Object[])}, but with a callback.
    // *
    // * @since 0.96.0
    // */
    // public <R> void batchCallback(Transaction transaction, final List<?
    // extends Row> actions, final Object[] results,
    // final Batch.Callback<R> callback) throws IOException,
    // InterruptedException;
    //
    // /**
    // * Same as {@link #batch(List)}, but with a callback.
    // *
    // * @since 0.96.0
    // */
    // public <R> Object[] batchCallback(List<? extends Row> actions,
    // Batch.Callback<R> callback) throws IOException,
    // InterruptedException;

    /**
     * Extracts certain cells from a given row.
     * 
     * @param get
     *            The object that specifies what data to fetch and from which
     *            row.
     * @return The data coming from the specified row, if it exists. If the row
     *         specified doesn't exist, the {@link Result} instance returned
     *         won't contain any {@link KeyValue}, as indicated by
     *         {@link Result#isEmpty()}.
     * @throws IOException
     *             if a remote or network exception occurs.
     * @since 0.20.0
     */
    Result get(Transaction transaction, Get get) throws IOException;

    /**
     * Extracts certain cells from the given rows, in batch.
     * 
     * @param gets
     *            The objects that specify what data to fetch and from which
     *            rows.
     * 
     * @return The data coming from the specified rows, if it exists. If the row
     *         specified doesn't exist, the {@link Result} instance returned
     *         won't contain any {@link KeyValue}, as indicated by
     *         {@link Result#isEmpty()}. If there are any failures even after
     *         retries, there will be a null in the results array for those
     *         Gets, AND an exception will be thrown.
     * @throws IOException
     *             if a remote or network exception occurs.
     * 
     * @since 0.90.0
     */
    Result[] get(Transaction transaction, List<Get> gets) throws IOException;

    /**
     * Returns a scanner on the current table as specified by the {@link Scan}
     * object. Note that the passed {@link Scan}'s start row and caching
     * properties maybe changed.
     * 
     * @param scan
     *            A configured {@link Scan} object.
     * @return A scanner.
     * @throws IOException
     *             if a remote or network exception occurs.
     * @since 0.20.0
     */
    ResultScanner getScanner(Transaction transaction, Scan scan) throws IOException;

    /**
     * Gets a scanner on the current table for the given family.
     * 
     * @param family
     *            The column family to scan.
     * @return A scanner.
     * @throws IOException
     *             if a remote or network exception occurs.
     * @since 0.20.0
     */
    ResultScanner getScanner(Transaction transaction, byte[] family) throws IOException;

    /**
     * Gets a scanner on the current table for the given family and qualifier.
     * 
     * @param family
     *            The column family to scan.
     * @param qualifier
     *            The column qualifier to scan.
     * @return A scanner.
     * @throws IOException
     *             if a remote or network exception occurs.
     * @since 0.20.0
     */
    ResultScanner getScanner(Transaction transaction, byte[] family, byte[] qualifier) throws IOException;

    /**
     * Puts some data in the table.
     * <p>
     * If {@link #isAutoFlush isAutoFlush} is false, the update is buffered
     * until the internal buffer is full.
     * 
     * @param put
     *            The data to put.
     * @throws IOException
     *             if a remote or network exception occurs.
     * @since 0.20.0
     */
    void put(Transaction transaction, Put put) throws IOException;

    /**
     * Puts some data in the table, in batch.
     * <p>
     * If {@link #isAutoFlush isAutoFlush} is false, the update is buffered
     * until the internal buffer is full.
     * <p>
     * This can be used for group commit, or for submitting user defined
     * batches. The writeBuffer will be periodically inspected while the List is
     * processed, so depending on the List size the writeBuffer may flush not at
     * all, or more than once.
     * 
     * @param puts
     *            The list of mutations to apply. The batch put is done by
     *            aggregating the iteration of the Puts over the write buffer at
     *            the client-side for a single RPC call.
     * @throws IOException
     *             if a remote or network exception occurs.
     * @since 0.20.0
     */
    void put(Transaction transaction, List<Put> puts) throws IOException;

    /**
     * Deletes the specified cells/row.
     * 
     * @param delete
     *            The object that specifies what to delete.
     * @throws IOException
     *             if a remote or network exception occurs.
     * @since 0.20.0
     */
    void delete(Transaction transaction, Delete delete) throws IOException;

    /**
     * Deletes the specified cells/rows in bulk.
     * 
     * @param deletes
     *            List of things to delete. List gets modified by this method
     *            (in particular it gets re-ordered, so the order in which the
     *            elements are inserted in the list gives no guarantee as to the
     *            order in which the {@link Delete}s are executed).
     * @throws IOException
     *             if a remote or network exception occurs. In that case the
     *             {@code deletes} argument will contain the {@link Delete}
     *             instances that have not be successfully applied.
     * @since 0.20.1
     */
    void delete(Transaction transaction, List<Delete> deletes) throws IOException;

    /**
     * Releases any resources held or pending changes in internal buffers.
     * 
     * @throws IOException
     *             if a remote or network exception occurs.
     */
    void close() throws IOException;

    /**
     * Provides access to the underliying HTable in order to configure it or to
     * perform unsafe (non-transactional) operations. The latter would break the
     * transactional guarantees of the whole system.
     * 
     * @return The underlying HTable object
     */
    public HTableInterface getHTable();
}
