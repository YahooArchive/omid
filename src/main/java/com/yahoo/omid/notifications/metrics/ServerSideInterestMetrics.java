package com.yahoo.omid.notifications.metrics;

import java.util.concurrent.TimeUnit;

import com.yahoo.omid.notifications.Interest;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class ServerSideInterestMetrics {

    Meter matchingRowsPerScanMeter;
    Timer scanTimer;
    private Timer offerTimer;

    public ServerSideInterestMetrics(Interest interest) {
        this.matchingRowsPerScanMeter = Metrics.newMeter(ServerSideInterestMetrics.class, interest
                + "-matchingRowsPerScan", interest + "-matchingRowsPerScan", TimeUnit.SECONDS);
        this.scanTimer = Metrics.newTimer(ServerSideInterestMetrics.class, interest + "-scanTimer",
                TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        this.offerTimer = Metrics.newTimer(ServerSideInterestMetrics.class, interest + "-offerTimer",
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

    public void offerTime(long elapsed) {
        offerTimer.update(elapsed, TimeUnit.MILLISECONDS);
    }
}
