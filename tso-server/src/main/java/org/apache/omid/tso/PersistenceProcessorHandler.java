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

import com.lmax.disruptor.WorkHandler;
import org.apache.omid.committable.CommitTable;
import org.apache.omid.metrics.Histogram;
import org.apache.omid.metrics.MetricsRegistry;
import org.apache.omid.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.codahale.metrics.MetricRegistry.name;

public class PersistenceProcessorHandler implements WorkHandler<PersistenceProcessorImpl.PersistBatchEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProcessorHandler.class);

    private final String tsoHostAndPort;
    private final LeaseManagement leaseManager;

    private final ReplyProcessor replyProcessor;
    private final RetryProcessor retryProc;
    private final CommitTable.Writer writer;
    final Panicker panicker;

    private final Timer flushTimer;
    private final Histogram batchSizeHistogram;

    @Inject
    PersistenceProcessorHandler(MetricsRegistry metrics,
                                String tsoHostAndPort,
                                LeaseManagement leaseManager,
                                CommitTable commitTable,
                                ReplyProcessor replyProcessor,
                                RetryProcessor retryProc,
                                Panicker panicker)
    throws InterruptedException, ExecutionException, IOException {

        this.tsoHostAndPort = tsoHostAndPort;
        this.leaseManager = leaseManager;
        this.writer = commitTable.getWriter();
        this.replyProcessor = replyProcessor;
        this.retryProc = retryProc;
        this.panicker = panicker;

        flushTimer = metrics.timer(name("tso", "persist", "flush"));
        batchSizeHistogram = metrics.histogram(name("tso", "persist", "batchsize"));

    }

    @Override
    public void onEvent(PersistenceProcessorImpl.PersistBatchEvent event) throws Exception {

        Batch batch = event.getBatch();
        for (int i=0; i < batch.getNumEvents(); ++i) {
            PersistEvent localEvent = batch.getEvent(i);

            switch (localEvent.getType()) {
            case COMMIT:
                localEvent.getMonCtx().timerStart("commitPersistProcessor");
                // TODO: What happens when the IOException is thrown?
                writer.addCommittedTransaction(localEvent.getStartTimestamp(), localEvent.getCommitTimestamp());
                break;
            case ABORT:
                break;
            case TIMESTAMP:
                localEvent.getMonCtx().timerStart("timestampPersistProcessor");
                break;
            case LOW_WATERMARK:
                writer.updateLowWatermark(localEvent.getLowWatermark());
                break;
            default:
                throw new RuntimeException("Unknown event type: " + localEvent.getType().name());
            }
        }
        flush(batch, event.getBatchID());

    }

    // TODO Fix this method with the contents of PersistenceProcessor.flush() in master branch
    // TODO This is related to the changes in TestPersistenceProcessor.testCommitPersistenceWithHALeaseManager().
    // TODO Check also that test in the master branch
    private void flush(Batch batch, long batchID) {

        if (batch.getNumEvents() > 0) {
            commitSuicideIfNotMaster();
            try {
                long startFlushTimeInNs = System.nanoTime();
                writer.flush();
                flushTimer.update(System.nanoTime() - startFlushTimeInNs);
                batchSizeHistogram.update(batch.getNumEvents());
            } catch (IOException e) {
                panicker.panic("Error persisting commit batch", e);
            }
            commitSuicideIfNotMaster(); // TODO Here, we can return the client responses before committing suicide
            batch.sendReply(replyProcessor, retryProc, batchID);
        }

    }

    private void commitSuicideIfNotMaster() {
        if (!leaseManager.stillInLeasePeriod()) {
            panicker.panic("Replica " + tsoHostAndPort + " lost mastership whilst flushing data. Committing suicide");
        }
    }

}