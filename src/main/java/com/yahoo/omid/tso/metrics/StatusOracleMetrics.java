package com.yahoo.omid.tso.metrics;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.yahoo.omid.notifications.ScannerSandbox.ScannerContainer.Scanner;
import com.yahoo.omid.tso.TSOHandler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class StatusOracleMetrics {

    private static Logger logger = Logger.getLogger(StatusOracleMetrics.class);

    private final Timer commitsTimer;
    private final Timer beginTimer;
    private final Timer commitsLatency;
    private final Meter committedMeter;
    private final Meter abortedMeter;
    private final Meter selfAbortedMeter;
    private final Meter queryMeter;
    private final Meter beginMeter;

    public StatusOracleMetrics() {
        committedMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@committed", "statusOracle",
                TimeUnit.SECONDS);
        abortedMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@aborted", "statusOracle",
                TimeUnit.SECONDS);
        selfAbortedMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@selfAborted",
                "statusOracle", TimeUnit.SECONDS);
        queryMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@query", "statusOracle",
                TimeUnit.SECONDS);
        beginMeter = Metrics.defaultRegistry().newMeter(TSOHandler.class, "statusOracle@begin",
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

    public void selfAborted() {
        selfAbortedMeter.mark();
    }

    public void query() {
        queryMeter.mark();
    }

    public void begin() {
        beginMeter.mark();
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
