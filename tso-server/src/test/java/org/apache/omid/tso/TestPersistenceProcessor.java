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

import org.apache.commons.pool2.ObjectPool;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.NullMetricsProvider;
import org.jboss.netty.channel.Channel;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.lmax.disruptor.BlockingWaitStrategy;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

// TODO Refactor: Make visible currentBatch in PersistenceProcessorImpl to add proper verifications
public class TestPersistenceProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(TestPersistenceProcessor.class);

    private static final long ANY_LWM = 1234L;
    private static final int ANY_ST = 0;
    private static final int ANY_CT = 1;

    @Mock
    private CommitTable.Writer mockWriter;
    @Mock
    private CommitTable.Client mockClient;
    @Mock
    private RetryProcessor retryProcessor;
    @Mock
    private Panicker panicker;

    private MetricsRegistry metrics;
    private CommitTable commitTable;

    @BeforeMethod(alwaysRun = true, timeOut = 30_000)
    public void initMocksAndComponents() throws Exception {

        MockitoAnnotations.initMocks(this);

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

    @Test(timeOut = 30_000)
    public void testLowWatermarkIsPersisted() throws Exception {

        TSOServerConfig tsoConfig = new TSOServerConfig();

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[tsoConfig.getNumConcurrentCTWriters()];
        for (int i = 0; i < tsoConfig.getNumConcurrentCTWriters(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          mock(LeaseManager.class),
                                                          commitTable,
                                                          mock(ReplyProcessor.class),
                                                          retryProcessor,
                                                          panicker);
        }

        // Component under test
        PersistenceProcessorImpl persistenceProcessor =
                new PersistenceProcessorImpl(tsoConfig,
                                             new BlockingWaitStrategy(),
                                             commitTable,
                                             mock(ObjectPool.class),
                                             panicker,
                                             handlers,
                                             metrics);

        persistenceProcessor.persistLowWatermark(ANY_LWM).get();

        ArgumentCaptor<Long> lwmCapture = ArgumentCaptor.forClass(Long.class);
        CommitTable.Writer lwmWriter = commitTable.getWriter();
        verify(lwmWriter, timeout(100).times(1)).updateLowWatermark(lwmCapture.capture());
        assertEquals(lwmCapture.getValue().longValue(), ANY_LWM);

    }

    @Test(timeOut = 30_000)
    public void testCommitPersistenceWithSingleCommitTableWriter() throws Exception {

        final int NUM_CT_WRITERS = 1;
        final int BATCH_SIZE_PER_CT_WRITER = 2;

        // Init a non-HA lease manager
        VoidLeaseManager leaseManager = spy(new VoidLeaseManager(mock(TSOChannelHandler.class),
                                                                 mock(TSOStateManager.class)));

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setBatchSizePerCTWriter(BATCH_SIZE_PER_CT_WRITER);
        tsoConfig.setNumConcurrentCTWriters(NUM_CT_WRITERS);

        ObjectPool<Batch> batchPool = spy(new BatchPoolModule(tsoConfig).getBatchPool());

        ReplyProcessor replyProcessor = new ReplyProcessorImpl(new BlockingWaitStrategy(), metrics, panicker, batchPool);

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[tsoConfig.getNumConcurrentCTWriters()];
        for (int i = 0; i < tsoConfig.getNumConcurrentCTWriters(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics, "localhost:1234",
                                                          leaseManager,
                                                          commitTable,
                                                          replyProcessor,
                                                          retryProcessor,
                                                          panicker);
        }

        // Component under test
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig, new BlockingWaitStrategy(), commitTable, batchPool,
                                                                     panicker, handlers, metrics);

        verify(batchPool, times(1)).borrowObject(); // Called during initialization

        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class));
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class)); // Flush: batch full
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class));
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class)); // Flush: batch full

        verify(batchPool, times(1 + BATCH_SIZE_PER_CT_WRITER)).borrowObject(); // 3: 1 in init + 2 when flushing

    }

    @Test(timeOut = 30_000)
    public void testCommitPersistenceWithMultipleCommitTableWriters() throws Exception {

        final int NUM_CT_WRITERS = 2;
        final int BATCH_SIZE_PER_CT_WRITER = 2;

        // Init a non-HA lease manager
        VoidLeaseManager leaseManager = spy(new VoidLeaseManager(mock(TSOChannelHandler.class),
                                                                 mock(TSOStateManager.class)));

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setBatchSizePerCTWriter(BATCH_SIZE_PER_CT_WRITER);
        tsoConfig.setNumConcurrentCTWriters(NUM_CT_WRITERS);

        ObjectPool<Batch> batchPool = spy(new BatchPoolModule(tsoConfig).getBatchPool());

        ReplyProcessor replyProcessor = new ReplyProcessorImpl(new BlockingWaitStrategy(), metrics, panicker, batchPool);

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[tsoConfig.getNumConcurrentCTWriters()];
        for (int i = 0; i < tsoConfig.getNumConcurrentCTWriters(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          leaseManager,
                                                          commitTable,
                                                          replyProcessor,
                                                          retryProcessor,
                                                          panicker);
        }

        // Component under test
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig, new BlockingWaitStrategy(), commitTable, batchPool,
                                                                     panicker, handlers, metrics);

        verify(batchPool, times(1)).borrowObject(); // Called during initialization

        // Fill 1st handler Batches completely
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class));
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class)); // 1st batch full
        verify(batchPool, times(2)).borrowObject();
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class));
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class)); // 2nd batch full
        verify(batchPool, times(3)).borrowObject();

        // Test empty flush does not trigger response in getting a new currentBatch
        proc.triggerCurrentBatchFlush();
        verify(batchPool, times(3)).borrowObject();

        // Fill 2nd handler Batches completely
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class));
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class)); // 1st batch full
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class));
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class)); // 2nd batch full
        verify(batchPool, times(1 + (NUM_CT_WRITERS * BATCH_SIZE_PER_CT_WRITER))).borrowObject();

        // Start filling a new currentBatch and flush it immediately
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class)); // Batch not full
        verify(batchPool, times(5)).borrowObject();
        proc.triggerCurrentBatchFlush(); // Flushing should provoke invocation of a new batch
        verify(batchPool, times(6)).borrowObject();

        // Test empty flush does not trigger response
        proc.triggerCurrentBatchFlush();
        proc.triggerCurrentBatchFlush();
        proc.triggerCurrentBatchFlush();
        proc.triggerCurrentBatchFlush();
        proc.triggerCurrentBatchFlush();
        verify(batchPool, times(6)).borrowObject();

    }

    @Test(timeOut = 30_000)
    public void testCommitPersistenceWithNonHALeaseManager() throws Exception {

        final int NUM_CT_WRITERS = 1;
        final int BATCH_SIZE_PER_CT_WRITER = 1;

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setBatchSizePerCTWriter(NUM_CT_WRITERS);
        tsoConfig.setNumConcurrentCTWriters(BATCH_SIZE_PER_CT_WRITER);
        tsoConfig.setBatchPersistTimeoutInMs(100);

        ObjectPool<Batch> batchPool = spy(new BatchPoolModule(tsoConfig).getBatchPool());

        ReplyProcessor replyProcessor = new ReplyProcessorImpl(new BlockingWaitStrategy(), metrics, panicker, batchPool);

        // Init a non-HA lease manager
        VoidLeaseManager leaseManager = spy(new VoidLeaseManager(mock(TSOChannelHandler.class),
                mock(TSOStateManager.class)));

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[tsoConfig.getNumConcurrentCTWriters()];
        for (int i = 0; i < tsoConfig.getNumConcurrentCTWriters(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          leaseManager,
                                                          commitTable,
                                                          replyProcessor,
                                                          retryProcessor,
                                                          panicker);
        }

        // Component under test
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig, new BlockingWaitStrategy(), commitTable, batchPool,
                                                                     panicker, handlers, metrics);

        // The non-ha lease manager always return true for
        // stillInLeasePeriod(), so verify the currentBatch sends replies as master
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class));
        proc.triggerCurrentBatchFlush();
        verify(leaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batchPool, times(2)).borrowObject();

    }

    @Test(timeOut = 30_000)
    public void testCommitPersistenceWithHALeaseManagerAndMinimumCommitTableWriters() throws Exception {

        final int NUM_PERSIST_HANDLERS = 2; // Minimum commit table writers is 2

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setNumConcurrentCTWriters(NUM_PERSIST_HANDLERS);

        testPersistenceWithHALeaseManagerPreservingLease(tsoConfig);
        testPersistenceWithHALeaseManagerFailingToPreserveLease1(tsoConfig);
        testPersistenceWithHALeaseManagerFailingToPreserveLease2(tsoConfig);
        testPersistenceWithHALeaseManagerFailingToPreserveLease3(tsoConfig);

    }

    @Test(timeOut = 30_000)
    public void testCommitPersistenceWithHALeaseManagerAndMultipleCommitTableWriters() throws Exception {

        final int NUM_CT_WRITERS = 4;
        final int BATCH_SIZE_PER_CT_WRITER = 4;

        TSOServerConfig tsoConfig = new TSOServerConfig();
        tsoConfig.setNumConcurrentCTWriters(NUM_CT_WRITERS);
        tsoConfig.setBatchSizePerCTWriter(BATCH_SIZE_PER_CT_WRITER);
        tsoConfig.setBatchPersistTimeoutInMs(100);

        testPersistenceWithHALeaseManagerPreservingLease(tsoConfig);
        testPersistenceWithHALeaseManagerFailingToPreserveLease1(tsoConfig);
        testPersistenceWithHALeaseManagerFailingToPreserveLease2(tsoConfig);
        testPersistenceWithHALeaseManagerFailingToPreserveLease3(tsoConfig);

    }

    private void testPersistenceWithHALeaseManagerPreservingLease(TSOServerConfig tsoConfig) throws Exception {

        // Init a HA lease manager
        LeaseManager simulatedHALeaseManager = mock(LeaseManager.class);

        ObjectPool<Batch> batchPool = spy(new BatchPoolModule(tsoConfig).getBatchPool());

        PersistenceProcessorHandler[] handlers = configureHandlers (tsoConfig, simulatedHALeaseManager, batchPool);

        // Component under test
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig, new BlockingWaitStrategy(), commitTable, batchPool,
                                                                     panicker, handlers, metrics);

        // Test: Configure the lease manager to return true always
        doReturn(true).when(simulatedHALeaseManager).stillInLeasePeriod();
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class));
        proc.triggerCurrentBatchFlush();
        verify(simulatedHALeaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batchPool, times(2)).borrowObject();
    }

    private void testPersistenceWithHALeaseManagerFailingToPreserveLease1(TSOServerConfig tsoConfig) throws Exception {

        // Init a HA lease manager
        LeaseManager simulatedHALeaseManager = mock(LeaseManager.class);

        ObjectPool<Batch> batchPool = spy(new BatchPoolModule(tsoConfig).getBatchPool());

        PersistenceProcessorHandler[] handlers = configureHandlers (tsoConfig, simulatedHALeaseManager, batchPool);

        // Component under test
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig, new BlockingWaitStrategy(), commitTable, batchPool,
                                                                     panicker, handlers, metrics);

        // Test: Configure the lease manager to return true first and false later for stillInLeasePeriod
        doReturn(true).doReturn(false).when(simulatedHALeaseManager).stillInLeasePeriod();
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class));
        proc.triggerCurrentBatchFlush();
        verify(simulatedHALeaseManager, timeout(1000).times(2)).stillInLeasePeriod();
        verify(batchPool, times(2)).borrowObject();
    }

    private void testPersistenceWithHALeaseManagerFailingToPreserveLease2(TSOServerConfig tsoConfig) throws Exception {

        // Init a HA lease manager
        LeaseManager simulatedHALeaseManager = mock(LeaseManager.class);

        ObjectPool<Batch> batchPool = spy(new BatchPoolModule(tsoConfig).getBatchPool());

        PersistenceProcessorHandler[] handlers = configureHandlers (tsoConfig, simulatedHALeaseManager, batchPool);

        // Component under test
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig, new BlockingWaitStrategy(), commitTable, batchPool,
                                                                     panicker, handlers, metrics);

        // Test: Configure the lease manager to return false for stillInLeasePeriod
        doReturn(false).when(simulatedHALeaseManager).stillInLeasePeriod();
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class));
        proc.triggerCurrentBatchFlush();
        verify(simulatedHALeaseManager, timeout(1000).times(1)).stillInLeasePeriod();
        verify(batchPool, times(2)).borrowObject();
    }

    private void testPersistenceWithHALeaseManagerFailingToPreserveLease3(TSOServerConfig tsoConfig) throws Exception {

        // Init a HA lease manager
        LeaseManager simulatedHALeaseManager = mock(LeaseManager.class);

        ObjectPool<Batch> batchPool = spy(new BatchPoolModule(tsoConfig).getBatchPool());

        PersistenceProcessorHandler[] handlers = configureHandlers (tsoConfig, simulatedHALeaseManager, batchPool);

        // Component under test
        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(tsoConfig, new BlockingWaitStrategy(), commitTable, batchPool,
                                                                     panicker, handlers, metrics);

        // Test: Configure the lease manager to return true first and false later for stillInLeasePeriod and raise
        // an exception when flush
        // Configure mock writer to flush unsuccessfully
        doThrow(new IOException("Unable to write")).when(mockWriter).flush();
        doReturn(true).doReturn(false).when(simulatedHALeaseManager).stillInLeasePeriod();
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), mock(MonitoringContext.class));
        proc.triggerCurrentBatchFlush();
        verify(simulatedHALeaseManager, timeout(1000).times(1)).stillInLeasePeriod();
        verify(batchPool, times(2)).borrowObject();

    }

    private PersistenceProcessorHandler[] configureHandlers(TSOServerConfig tsoConfig,
                                                            LeaseManager leaseManager,
                                                            ObjectPool<Batch> batchPool)
            throws Exception {
        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[tsoConfig.getNumConcurrentCTWriters()];
        for (int i = 0; i < tsoConfig.getNumConcurrentCTWriters(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          leaseManager,
                                                          commitTable,
                                                          new ReplyProcessorImpl(new BlockingWaitStrategy(), metrics, panicker, batchPool),
                                                          retryProcessor,
                                                          new RuntimeExceptionPanicker());
        }
        return handlers;
    }

    @Test(timeOut = 30_000)
    public void testCommitTableExceptionOnCommitPersistenceTakesDownDaemon() throws Exception {

        // Init lease management (doesn't matter if HA or not)
        LeaseManagement leaseManager = mock(LeaseManagement.class);

        TSOServerConfig config = new TSOServerConfig();

        ObjectPool<Batch> batchPool = spy(new BatchPoolModule(config).getBatchPool());

        ReplyProcessor replyProcessor = new ReplyProcessorImpl(new BlockingWaitStrategy(), metrics, panicker, batchPool);

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[config.getNumConcurrentCTWriters()];
        for (int i = 0; i < config.getNumConcurrentCTWriters(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          leaseManager,
                                                          commitTable,
                                                          replyProcessor,
                                                          mock(RetryProcessor.class),
                                                          panicker);
        }

        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(config, new BlockingWaitStrategy(), commitTable, batchPool,
                                                                     panicker, handlers, metrics);

        MonitoringContext monCtx = new MonitoringContext(metrics);

        // Configure lease manager to work normally
        doReturn(true).when(leaseManager).stillInLeasePeriod();

        // Configure commit table writer to explode when flushing changes to DB
        doThrow(new IOException("Unable to write@TestPersistenceProcessor2")).when(mockWriter).flush();

        // Check the panic is extended!
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), monCtx);
        proc.triggerCurrentBatchFlush();
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));

    }

    @Test(timeOut = 30_000)
    public void testRuntimeExceptionOnCommitPersistenceTakesDownDaemon() throws Exception {

        TSOServerConfig config = new TSOServerConfig();

        ObjectPool<Batch> batchPool = new BatchPoolModule(config).getBatchPool();

        ReplyProcessor replyProcessor = new ReplyProcessorImpl(new BlockingWaitStrategy(), metrics, panicker, batchPool);

        PersistenceProcessorHandler[] handlers = new PersistenceProcessorHandler[config.getNumConcurrentCTWriters()];
        for (int i = 0; i < config.getNumConcurrentCTWriters(); i++) {
            handlers[i] = new PersistenceProcessorHandler(metrics,
                                                          "localhost:1234",
                                                          mock(LeaseManager.class),
                                                          commitTable,
                                                          replyProcessor,
                                                          retryProcessor,
                                                          panicker);
        }

        PersistenceProcessorImpl proc = new PersistenceProcessorImpl(config, new BlockingWaitStrategy(), commitTable, batchPool,
                                                                     panicker, handlers, metrics);

        // Configure writer to explode with a runtime exception
        doThrow(new RuntimeException("Kaboom!")).when(mockWriter).addCommittedTransaction(anyLong(), anyLong());
        MonitoringContext monCtx = new MonitoringContext(metrics);

        // Check the panic is extended!
        proc.addCommitToBatch(ANY_ST, ANY_CT, mock(Channel.class), monCtx);
        proc.triggerCurrentBatchFlush();
        verify(panicker, timeout(1000).atLeastOnce()).panic(anyString(), any(Throwable.class));

    }

}
