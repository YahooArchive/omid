package com.yahoo.omid.tso.metrics;

import java.util.concurrent.TimeUnit;

import com.yahoo.omid.tso.TSOHandler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class StatusOracleMetrics {

    private final Timer commitsTimer;
    private final Timer beginTimer;
    private final Timer commitsLatency;
    private final Meter committedMeter;
    private final Meter abortedMeter;
    private final Meter oldAbortedMeter;
    private final Meter selfAbortedMeter;
    private final Meter queryMeter;
    private final Meter beginMeter;
    private final Meter cleanedAbortMeter;
    private final Meter tooOldTimestampMeter;

    public StatusOracleMetrics() {
        committedMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@committed", "statusOracle",
                TimeUnit.SECONDS);
        abortedMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@aborted", "statusOracle",
                TimeUnit.SECONDS);
        selfAbortedMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@selfAborted",
                "statusOracle", TimeUnit.SECONDS);
        oldAbortedMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@oldAborted",
                "statusOracle", TimeUnit.SECONDS);
        queryMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@query", "statusOracle",
                TimeUnit.SECONDS);
        beginMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@begin",
                "statusOracle", TimeUnit.SECONDS);
        tooOldTimestampMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@tooOldTimestamp",
                "statusOracle", TimeUnit.SECONDS);
        cleanedAbortMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@cleanedAbort",
                "statusOracle", TimeUnit.SECONDS);
        commitsTimer = Metrics.defaultRegistry().newTimer(TSOHandler.class, "statusOracle@commit-processingTime",
                "statusOracle");
        commitsLatency = Metrics.defaultRegistry().newTimer(TSOHandler.class, "statusOracle@commit-latency",
                "statusOracle");
        beginTimer = Metrics.defaultRegistry().newTimer(TSOHandler.class, "statusOracle@begin-processingTime",
                "statusOracle");
    }

    public void commited() {
        committedMeter.mark();
    }

    public void aborted() {
        abortedMeter.mark();
    }

    public void oldAborted(long n) {
        oldAbortedMeter.mark(n);
    }

    public void selfAborted() {
        selfAbortedMeter.mark();
    }

    public void cleanedAbort() {
        cleanedAbortMeter.mark();
    }

    public void query() {
        queryMeter.mark();
    }

    public void begin() {
        beginMeter.mark();
    }
    
    public void tooOldTimestamp() {
        tooOldTimestampMeter.mark();
    }

    public TimerContext startCommitProcessing() {
        return commitsTimer.time();
    }

    public TimerContext startCommitLatency() {
        return commitsLatency.time();
    }

    public TimerContext startBeginProcessing() {
        return beginTimer.time();
    }
}
