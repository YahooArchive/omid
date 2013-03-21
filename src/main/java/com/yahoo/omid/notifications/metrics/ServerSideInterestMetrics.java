package com.yahoo.omid.notifications.metrics;

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class ServerSideInterestMetrics {

    private Gauge scannedRows;
    private Meter matchingRowsPerScanMeter;
    private Timer scanTimer;
    private String interestName;

    public ServerSideInterestMetrics(String interestName) {
        this.interestName = interestName;
        this.scannedRows = Metrics.newGauge(ServerSideInterestMetrics.class, interestName + "-scannedRows");
        this.matchingRowsPerScanMeter = Metrics.newMeter(ServerSideInterestMetrics.class, interestName
                + "-matchingRowsPerScan", interestName + "-matchingRowsPerScan", TimeUnit.SECONDS);
        this.scanTimer = Metrics.newTimer(ServerSideInterestMetrics.class, interestName + "-scanTimer",
                TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    }

    public void matched(long count) {
        matchingRowsPerScanMeter.mark(count);
    }

    public TimerContext scanStart() {
        return scanTimer.time();
    }

    public void scanEnd(TimerContext timer) {
        timer.stop();
    }
}
