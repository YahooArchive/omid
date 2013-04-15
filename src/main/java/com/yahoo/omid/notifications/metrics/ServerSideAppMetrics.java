package com.yahoo.omid.notifications.metrics;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.beust.jcommander.internal.Maps;
import com.yahoo.omid.notifications.AppInstanceNotifier;
import com.yahoo.omid.notifications.ScannerSandbox.ScannerContainer.Scanner;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class ServerSideAppMetrics {

    private static Logger logger = Logger.getLogger(ServerSideAppMetrics.class);

    private Meter notificationsMeter;
    private Map<String, Timer> interestsTimers;

    public ServerSideAppMetrics(String appName, Set<String> interests) {
        notificationsMeter = Metrics.defaultRegistry().newMeter(Scanner.class, appName + "@notifications-sent",
                "notifications", TimeUnit.SECONDS);
        interestsTimers = Maps.newHashMap();
        for (String interest : interests) {
            interestsTimers.put(interest,
                    Metrics.newTimer(AppInstanceNotifier.class, appName + "@" + interest + "-notification-send-time"));
        }
    }

    public void notificationSentEvent() {
        notificationsMeter.mark();
    }

    public TimerContext startNotificationSendTimer(String interest) {
        return interestsTimers.get(interest).time();
    }

}
