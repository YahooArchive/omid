package com.yahoo.omid.tso;

import java.io.IOException;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

public class PausableTimestampOracle extends TimestampOracleImpl {

    private static final Logger LOG = LoggerFactory.getLogger(PausableTimestampOracle.class);

    private volatile boolean tsoPaused = false;

    @Inject
    public PausableTimestampOracle(MetricRegistry metrics,
                                   TimestampStorage tsStorage,
                                   Panicker panicker) throws IOException {
        super(metrics, tsStorage, panicker);
    }

    @Override
    public long next() throws IOException {
        while (tsoPaused) {
            synchronized (this) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    LOG.error("Interrupted whilst paused");
                    Thread.currentThread().interrupt();
                }
            }
        }
        return super.next();
    }

    public synchronized void pause() {
        tsoPaused = true;
        this.notifyAll();
    }

    public synchronized void resume() {
        tsoPaused = false;
        this.notifyAll();
    }

    public boolean isTSOPaused() {
        return tsoPaused;
    }

}
