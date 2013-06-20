package com.yahoo.omid.notifications.metrics;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.beust.jcommander.internal.Maps;
import com.yahoo.omid.notifications.ScannerSandbox.ScannerContainer.Scanner;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

public class ServerSideAppMetrics {

    private static Logger logger = Logger.getLogger(ServerSideAppMetrics.class);

    private Map<String, Meter> notificationsMeters;
    private Map<String, Histogram> notificationsHistograms;

    public ServerSideAppMetrics(String appName, Collection<String> observers) {
        notificationsMeters = Maps.newHashMap();
        notificationsHistograms = Maps.newHashMap();
        for (String observer : observers) {
            notificationsMeters.put(
                    observer,
                    Metrics.defaultRegistry().newMeter(Scanner.class, appName + "@" + observer + "-notifications-sent",
                            "notifications", TimeUnit.SECONDS));
            notificationsHistograms.put(
                    observer,
                    Metrics.defaultRegistry().newHistogram(Scanner.class,
                            appName + "@" + observer + "-notifications-sent-hist", "notifications", true));
        }
    }

    public void notificationSentEvent(String observer, long count) {
        notificationsMeters.get(observer).mark(count);
        notificationsHistograms.get(observer).update(count);
    }

}
