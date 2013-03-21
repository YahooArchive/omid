package com.yahoo.omid.notifications.metrics;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.yahoo.omid.notifications.ScannerSandbox.ScannerContainer.Scanner;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;

public class ServerSideAppMetrics {

    private static Logger logger = Logger.getLogger(ServerSideAppMetrics.class);

    private Meter notificationsMeter;

    public ServerSideAppMetrics(String appName) {
        notificationsMeter = Metrics.defaultRegistry().newMeter(Scanner.class, appName + "@notifications-sent",
                "notifications", TimeUnit.SECONDS);
    }

    public void notificationSentEvent() {
        notificationsMeter.mark();
    }

}
