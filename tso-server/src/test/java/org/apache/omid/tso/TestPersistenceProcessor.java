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
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.jboss.netty.channel.Channel;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

// TODO Add timers
// TODO Make visible currentBatch in PersistenceProcessorImpl to add proper verifications
public class TestPersistenceProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TestPersistenceProcessor.class);

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
//        doThrow(new IOException("Unable to write")).when(mockWriter).flush();

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
    public void testCommitPersistenceWithMultiHandlers() throws Exception {

        final int MAX_BATCH_SIZE = 4;
        final int NUM_PERSIST_HANDLERS = 4;

        // Init a non-HA lease manager
        VoidLeaseManager leaseManager = spy(new VoidLeaseManager(mock(TSOChannelHandler.class),
                                                                 mock(TSOStateManager.class)));

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setMaxBatchSize(MAX_BATCH_SIZE);
        tsoConfig.setPersistHandlerNum(NUM_PERSIST_HANDLERS);

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[tsoConfig.getPersistHandlerNum()];
        for (int i = 0; i < tsoConfig.getPersistHandlerNum(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics, "localhost:1234",
                                                          leaseManager,
                                                          commitTable,
                                                          replyProcessor,
                                                          retryProcessor,
                                                          panicker);
        }

        // Component under test
        BatchPool batchPool = spy(new BatchPool(tsoConfig));

        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig,
                                                                     batchPool,
                                                                     replyProcessor,
                                                                     panicker,
                                                                     handlers);

        verify(batchPool, times(1)).getNextEmptyBatch();

        proc.persistCommit(1, 2, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistFlush();

        proc.persistCommit(3, 4, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistFlush();

        proc.persistCommit(5, 6, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistFlush();

        proc.persistCommit(7, 8, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistFlush();

        verify(batchPool, times(5)).getNextEmptyBatch(); // 5 Times: 1 in initialization + 4 when flushing above

    }

    @Test
    public void testCommitPersistenceWithSingleHandlerInMultiHandlersEnvironment() throws Exception {

        final int MAX_BATCH_SIZE = 16;
        final int NUM_PERSIST_HANDLERS = 4;

        // Init a non-HA lease manager
        VoidLeaseManager leaseManager = spy(new VoidLeaseManager(mock(TSOChannelHandler.class),
                                                                 mock(TSOStateManager.class)));

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setMaxBatchSize(MAX_BATCH_SIZE);
        tsoConfig.setPersistHandlerNum(NUM_PERSIST_HANDLERS);

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[tsoConfig.getPersistHandlerNum()];
        for (int i = 0; i < tsoConfig.getPersistHandlerNum(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          leaseManager,
                                                          commitTable,
                                                          replyProcessor,
                                                          retryProcessor,
                                                          panicker);
        }

        // Component under test
        BatchPool batchPool = spy(new BatchPool(tsoConfig));
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig,
                                                                     batchPool,
                                                                     replyProcessor,
                                                                     panicker,
                                                                     handlers);

        verify(batchPool, times(1)).getNextEmptyBatch();

        // Fill one Batch completely
        proc.persistCommit(1, 2, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistCommit(3, 4, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistCommit(5, 6, mock(Channel.class), mock(MonitoringContext.class));
        verify(batchPool, times(1)).getNextEmptyBatch();
        proc.persistCommit(7, 8, mock(Channel.class), mock(MonitoringContext.class)); // Should be full here
        verify(batchPool, times(2)).getNextEmptyBatch();

        // Test empty flush does not trigger response in getting a new batch
        proc.persistFlush();
        verify(batchPool, times(2)).getNextEmptyBatch();

        // Fill a second Batch completely
        proc.persistCommit(1, 2, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistCommit(3, 4, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistCommit(5, 6, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistCommit(7, 8, mock(Channel.class), mock(MonitoringContext.class)); // Should be full here
        verify(batchPool, times(3)).getNextEmptyBatch();

        // Start filling a new batch and flush it immediately
        proc.persistCommit(9, 10, mock(Channel.class), mock(MonitoringContext.class));
        verify(batchPool, times(3)).getNextEmptyBatch();
        proc.persistFlush();
        verify(batchPool, times(4)).getNextEmptyBatch();

        // Test empty flush does not trigger response
        proc.persistFlush();
        proc.persistFlush();
        proc.persistFlush();
        proc.persistFlush();
        proc.persistFlush();
        verify(batchPool, times(4)).getNextEmptyBatch();

    }

    @Test
    public void testCommitPersistenceWithNonHALeaseManager() throws Exception {

        final int MAX_BATCH_SIZE = 4;
        final int NUM_PERSIST_HANDLERS = 4;

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setMaxBatchSize(MAX_BATCH_SIZE);
        tsoConfig.setPersistHandlerNum(NUM_PERSIST_HANDLERS);
        tsoConfig.setBatchPersistTimeoutInMs(100);

        // Init a non-HA lease manager
        VoidLeaseManager leaseManager = spy(new VoidLeaseManager(mock(TSOChannelHandler.class),
                mock(TSOStateManager.class)));

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[tsoConfig.getPersistHandlerNum()];
        for (int i = 0; i < tsoConfig.getPersistHandlerNum(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          leaseManager,
                                                          commitTable,
                                                          replyProcessor,
                                                          retryProcessor,
                                                          panicker);
        }

        // Component under test
        BatchPool batchPool = spy(new BatchPool(tsoConfig));
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig,
                                                                     batchPool,
                                                                     replyProcessor,
                                                                     panicker,
                                                                     handlers);

        // The non-ha lease manager always return true for
        // stillInLeasePeriod(), so verify the batch sends replies as master
        proc.persistCommit(1, 2, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistFlush();
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batchPool, times(2)).getNextEmptyBatch();

    }

    @Test
    public void testCommitPersistenceWithHALeaseManagerMultiHandlers() throws Exception {
        final int MAX_BATCH_SIZE = 4;
        final int NUM_PERSIST_HANDLERS = 4;

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setMaxBatchSize(MAX_BATCH_SIZE);
        tsoConfig.setPersistHandlerNum(NUM_PERSIST_HANDLERS);
        tsoConfig.setBatchPersistTimeoutInMs(100);

        testCommitPersistenceWithHALeaseManagerPerConfig(tsoConfig);
    }

    @Test
    public void testCommitPersistenceWithHALeaseManager() throws Exception {

        TSOServerConfig tsoConfig = new TSOServerConfig();
        testCommitPersistenceWithHALeaseManagerPerConfig(tsoConfig);

    }

    // TODO Recheck this tests comparing with previous master
    private void testCommitPersistenceWithHALeaseManagerPerConfig (TSOServerConfig tsoConfig) throws Exception {

        // Init a HA lease manager
        LeaseManager leaseManager = mock(LeaseManager.class);

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[tsoConfig.getPersistHandlerNum()];
        for (int i = 0; i < tsoConfig.getPersistHandlerNum(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          leaseManager,
                                                          commitTable,
                                                          replyProcessor,
                                                          retryProcessor,
                                                          new RuntimeExceptionPanicker());
        }

        // Component under test
        BatchPool batchPool = spy(new BatchPool(tsoConfig));
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig,
                                                                     batchPool,
                                                                     replyProcessor,
                                                                     panicker,
                                                                     handlers);

        doReturn(true).when(leaseManager).stillInLeasePeriod();
        MonitoringContext monCtx = new MonitoringContext(metrics);
        proc.persistCommit(1, 2, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistFlush();
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batchPool, times(2)).getNextEmptyBatch();

        // Test 2: Configure the lease manager to return true first and false later for stillInLeasePeriod

        // Reset stuff
        reset(leaseManager);
        doReturn(true).doReturn(false).when(leaseManager).stillInLeasePeriod();
        proc.persistCommit(1, 2, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistFlush();
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batchPool, times(3)).getNextEmptyBatch();

        // Test 3: Configure the lease manager to return false for stillInLeasePeriod

        // Reset stuff
        reset(leaseManager);
        doReturn(false).when(leaseManager).stillInLeasePeriod();
        proc.persistCommit(1, 2, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistFlush();
        verify(leaseManager, timeout(1000).times(1)).stillInLeasePeriod();
        verify(batchPool, times(4)).getNextEmptyBatch();

        // Test 4: Configure the lease manager to return true first and false later for stillInLeasePeriod and raise
        // an exception when flush

        // Reset stuff
        reset(leaseManager);
        // Configure mock writer to flush successfully
        doThrow(new IOException("Unable to write")).when(mockWriter).flush();
        doReturn(true).doReturn(false).when(leaseManager).stillInLeasePeriod();
        proc.persistCommit(1, 2, mock(Channel.class), mock(MonitoringContext.class));
        proc.persistFlush();
        verify(leaseManager, timeout(1000).times(1)).stillInLeasePeriod();
        verify(batchPool, times(5)).getNextEmptyBatch();
    }

    @Test
    public void testCommitTableExceptionOnCommitPersistenceTakesDownDaemon() throws Exception {

        // Init lease management (doesn't matter if HA or not)
        LeaseManagement leaseManager = mock(LeaseManagement.class);
        TSOServerConfig config = new TSOServerConfig();
        BatchPool batchPool = new BatchPool(config);

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[config.getPersistHandlerNum()];
        for (int i = 0; i < config.getPersistHandlerNum(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          leaseManager,
                                                          commitTable,
                                                          mock(ReplyProcessor.class),
                                                          mock(RetryProcessor.class),
                                                          panicker);
        }

        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(config,
                                                                 batchPool,
                                                                 mock(ReplyProcessor.class),
                                                                 panicker,
                                                                 handlers);

        MonitoringContext monCtx = new MonitoringContext(metrics);

        // Configure lease manager to work normally
        doReturn(true).when(leaseManager).stillInLeasePeriod();

        // Configure commit table writer to explode when flushing changes to DB
        doThrow(new IOException("Unable to write@TestPersistenceProcessor2")).when(mockWriter).flush();

        // Check the panic is extended!
        proc.persistCommit(1, 2, null, monCtx);
        proc.persistFlush();
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));
    }

    @Test
    public void testRuntimeExceptionOnCommitPersistenceTakesDownDaemon() throws Exception {

        TSOServerConfig config = new TSOServerConfig();
        BatchPool batchPool = new BatchPool(config);

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[config.getPersistHandlerNum()];
        for (int i = 0; i < config.getPersistHandlerNum(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          mock(LeaseManager.class),
                                                          commitTable,
                                                          replyProcessor,
                                                          retryProcessor,
                                                          panicker);
        }

        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(config,
                                                                 batchPool,
                                                                 replyProcessor,
                                                                 panicker,
                                                                 handlers);

        // Configure writer to explode with a runtime exception
        doThrow(new RuntimeException("Kaboom!")).when(mockWriter).addCommittedTransaction(anyLong(), anyLong());
        MonitoringContext monCtx = new MonitoringContext(metrics);

        // Check the panic is extended!
        proc.persistCommit(1, 2, null, monCtx);
        proc.persistFlush();
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));

    }

}
