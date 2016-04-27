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
package org.apache.omid.tso;

import org.apache.omid.committable.CommitTable;
import org.apache.omid.committable.InMemoryCommitTable;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.apache.omid.tso.PersistenceProcessorImpl.Batch;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@SuppressWarnings({"UnusedDeclaration"})
public class TestPersistenceProcessor {

    @Mock
    private Batch batch;
    @Mock
    private CommitTable.Writer mockWriter;
    @Mock
    private CommitTable.Client mockClient;
    @Mock
    private RetryProcessor retryProcessor;
    @Mock
    private ReplyProcessor replyProcessor;
    @Mock
    private Panicker panicker;

    private MetricsRegistry metrics;
    private CommitTable commitTable;

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void initMocksAndComponents() throws Exception {

        MockitoAnnotations.initMocks(this);

        // Configure mock writer to flush successfully
        doThrow(new IOException("Unable to write")).when(mockWriter).flush();

        // Configure null metrics provider
        metrics = new NullMetricsProvider();

        // Configure commit table to return the mocked writer and client
        commitTable = new CommitTable() {
            @Override
            public Writer getWriter() {
                return mockWriter;
            }

            @Override
            public Client getClient() {
                return mockClient;
            }
        };
    }

    @AfterMethod
    void afterMethod() {
        Mockito.reset(mockWriter);
    }

    @Test
    public void testCommitPersistenceWithNonHALeaseManager() throws Exception {

        // Init a non-HA lease manager
        VoidLeaseManager leaseManager = spy(new VoidLeaseManager(mock(TSOChannelHandler.class),
                                                                 mock(TSOStateManager.class)));

        TSOServerConfig tsoServerConfig = new TSOServerConfig();
        tsoServerConfig.setBatchPersistTimeoutInMs(100);

        Batch batch = spy(new Batch(1));

        // Component under test
        PersistenceProcessor proc = new PersistenceProcessorImpl(tsoServerConfig,
                                                                 metrics,
                                                                 "localhost:1234",
                                                                 batch,
                                                                 leaseManager,
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker);

        // The non-ha lease manager always return true for
        // stillInLeasePeriod(), so verify the batch sends replies as master
        MonitoringContext monCtx = new MonitoringContext(metrics);
        proc.persistCommit(0, 1, null, monCtx);
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batch, timeout(1000).times(1)).sendRepliesAndReset(any(ReplyProcessor.class),
                                                                  any(RetryProcessor.class));
    }

    @Test
    public void testCommitPersistenceWithHALeaseManager() throws Exception {

        TSOServerConfig tsoServerConfig = new TSOServerConfig();
        tsoServerConfig.setBatchPersistTimeoutInMs(100);

        MonitoringContext monCtx = new MonitoringContext(metrics);

        // Test 1: Configure the lease manager to always return true for stillInLeasePeriod, so verify the batch sends
        // replies as master

        // Init stuff
        ArgumentCaptor<String> msg = ArgumentCaptor.forClass(String.class);
        Panicker panicker = spy(new RuntimeExceptionPanicker());
        LeaseManager leaseManager = mock(LeaseManager.class);
        Batch batch = spy(new Batch(1));
        // Component under test
        PersistenceProcessor proc = new PersistenceProcessorImpl(tsoServerConfig,
                                                                 metrics,
                                                                 "localhost:1234",
                                                                 batch,
                                                                 leaseManager,
                                                                 new InMemoryCommitTable(),
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker);

        doReturn(true).when(leaseManager).stillInLeasePeriod();
        proc.persistCommit(1, 2, null, monCtx);
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batch).sendRepliesAndReset(any(ReplyProcessor.class), any(RetryProcessor.class));
        verify(panicker, never()).panic(msg.capture(), any(Throwable.class));

        // Test 2: Configure the lease manager to return true first and false later for stillInLeasePeriod, so verify
        // the batch sends replies as non-master

        // Reset stuff
        reset(leaseManager);
        batch = spy(new Batch(1));
        msg = ArgumentCaptor.forClass(String.class);
        panicker = spy(new RuntimeExceptionPanicker());
        // Component under test
        proc = new PersistenceProcessorImpl(tsoServerConfig,
                                            metrics,
                                            "localhost:1234",
                                            batch,
                                            leaseManager,
                                            new InMemoryCommitTable(),
                                            replyProcessor,
                                            retryProcessor,
                                            panicker);
        doReturn(true).doReturn(false).when(leaseManager).stillInLeasePeriod();
        proc.persistCommit(1, 2, null, monCtx);
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batch, never()).sendRepliesAndReset(any(ReplyProcessor.class), any(RetryProcessor.class));
        verify(panicker).panic(msg.capture());
        assertTrue(msg.getValue().contains("Committing suicide"));

        // Test 3:  Configure the lease manager to always return false for stillInLeasePeriod, so verify the batch
        // sends replies as non-master

        // Reset stuff
        reset(leaseManager);
        batch = spy(new Batch(1));
        msg = ArgumentCaptor.forClass(String.class);
        panicker = spy(new RuntimeExceptionPanicker());
        // Component under test
        proc = new PersistenceProcessorImpl(tsoServerConfig,
                                            metrics,
                                            "localhost:1234",
                                            batch,
                                            leaseManager,
                                            commitTable,
                                            replyProcessor,
                                            retryProcessor,
                                            panicker);
        doReturn(false).when(leaseManager).stillInLeasePeriod();
        proc.persistCommit(1, 2, null, monCtx);
        verify(leaseManager, timeout(1000).times(1)).stillInLeasePeriod();
        verify(batch, never()).sendRepliesAndReset(any(ReplyProcessor.class), any(RetryProcessor.class));
        verify(panicker).panic(msg.capture());
        assertTrue(msg.getValue().contains("Committing suicide"));

        // Test 4: Test when flushing data fails to the database

        // Reset stuff
        reset(leaseManager);
        batch = spy(new Batch(1));
        msg = ArgumentCaptor.forClass(String.class);
        panicker = spy(new RuntimeExceptionPanicker());
        // Component under test
        proc = new PersistenceProcessorImpl(tsoServerConfig,
                                            metrics,
                                            "localhost:1234",
                                            batch,
                                            leaseManager,
                                            commitTable, // Commit table writer will fail and launch a panic
                                            replyProcessor,
                                            retryProcessor,
                                            panicker);
        doReturn(true).when(leaseManager).stillInLeasePeriod();
        proc.persistCommit(1, 2, null, monCtx);
        verify(leaseManager, timeout(1000).times(1)).stillInLeasePeriod();
        verify(batch, never()).sendRepliesAndReset(any(ReplyProcessor.class), any(RetryProcessor.class));
        verify(panicker).panic(msg.capture(), isA(IOException.class));
        assertEquals(msg.getValue(), "Error persisting commit batch");

    }

    @Test
    public void testCommitTableExceptionOnCommitPersistenceTakesDownDaemon() throws Exception {

        // Init lease management (doesn't matter if HA or not)
        LeaseManagement leaseManager = mock(LeaseManagement.class);
        PersistenceProcessor proc = new PersistenceProcessorImpl(new TSOServerConfig(),
                                                                 metrics,
                                                                 "localhost:1234",
                                                                 leaseManager,
                                                                 commitTable,
                                                                 mock(ReplyProcessor.class),
                                                                 mock(RetryProcessor.class),
                                                                 panicker);
        MonitoringContext monCtx = new MonitoringContext(metrics);

        // Configure lease manager to work normally
        doReturn(true).when(leaseManager).stillInLeasePeriod();

        // Configure commit table writer to explode when flushing changes to DB
        doThrow(new IOException("Unable to write@TestPersistenceProcessor2")).when(mockWriter).flush();

        // Check the panic is extended!
        proc.persistCommit(1, 2, null, monCtx);
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));

    }

    @Test
    public void testRuntimeExceptionOnCommitPersistenceTakesDownDaemon() throws Exception {

        PersistenceProcessor proc = new PersistenceProcessorImpl(new TSOServerConfig(),
                                                                 metrics,
                                                                 "localhost:1234",
                                                                 mock(LeaseManagement.class),
                                                                 commitTable,
                                                                 replyProcessor,
                                                                 retryProcessor,
                                                                 panicker);

        // Configure writer to explode with a runtime exception
        doThrow(new RuntimeException("Kaboom!")).when(mockWriter).addCommittedTransaction(anyLong(), anyLong());
        MonitoringContext monCtx = new MonitoringContext(metrics);

        // Check the panic is extended!
        proc.persistCommit(1, 2, null, monCtx);
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));

    }

}
