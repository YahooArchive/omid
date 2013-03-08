package com.yahoo.omid.notifications.metrics;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.yahoo.omid.notifications.ScannerSandbox.ScannerContainer.Scanner;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.reporting.ConsoleReporter;

public class ServerSideAppMetrics {

    private static Logger logger = Logger.getLogger(ServerSideAppMetrics.class);

    private Meter notificationsMeter;
    
    public ServerSideAppMetrics(String appName) {
        long period = Long.valueOf(10);
        TimeUnit timeUnit = TimeUnit.valueOf("SECONDS");
        logger.info("Reporting metrics on the console with frequency of " + period + timeUnit.name());
        ConsoleReporter.enable(period, timeUnit);
        
        notificationsMeter = Metrics.newMeter(Scanner.class, appName + "@notifications-sent", "notifications", TimeUnit.SECONDS);
    }


    public void notificationSentEvent() {
        notificationsMeter.mark();
    }

}
