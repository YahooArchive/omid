package com.yahoo.omid.notifications.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import com.yahoo.omid.notifications.client.NotificationManager;
import com.yahoo.omid.notifications.client.ObserverWrapper;
import com.yahoo.omid.notifications.conf.ClientConfiguration;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class ClientSideAppMetrics {

    private static org.slf4j.Logger logger = LoggerFactory.getLogger(ClientSideAppMetrics.class);

    private String appName;

    private Meter notificationsMeter;

    private Map<String, Meter> observerInvocationMeters = new ConcurrentHashMap<String, Meter>(); // This is redundant
                                                                                                  // with the counter
                                                                                                  // offered by
                                                                                                  // observerExexutionTimers
    private Map<String, Meter> observerCompletionMeters = new ConcurrentHashMap<String, Meter>();
    private Map<String, Meter> observerAbortMeters = new ConcurrentHashMap<String, Meter>();
    private Map<String, Meter> omidAbortMeters = new ConcurrentHashMap<String, Meter>();
    private Map<String, Meter> unknownAbortMeters = new ConcurrentHashMap<String, Meter>();
    private Map<String, Timer> observerExecutionTimers = new ConcurrentHashMap<String, Timer>();

    public ClientSideAppMetrics(String appName, ClientConfiguration conf) {
        this.appName = appName;
        String metricsConfig = conf.getString("omid.metrics");
        logger.info("metrics= {}", metricsConfig);
        MetricsUtils.initMetrics(metricsConfig);
        notificationsMeter = Metrics.defaultRegistry().newMeter(NotificationManager.class,
                this.appName + "@notifications-received", "notifications", TimeUnit.SECONDS);
    }

    public void addObserver(String obsName) {
        observerInvocationMeters.put(obsName, Metrics.newMeter(ObserverWrapper.class, this.appName + "_" + obsName
                + "@invocations", "invocations", TimeUnit.SECONDS));
        observerCompletionMeters.put(obsName, Metrics.newMeter(ObserverWrapper.class, this.appName + "_" + obsName
                + "@completions", "completions", TimeUnit.SECONDS));
        observerAbortMeters.put(obsName, Metrics.newMeter(ObserverWrapper.class, this.appName + "_" + obsName
                + "@aborts", "aborts", TimeUnit.SECONDS));
        omidAbortMeters.put(obsName, Metrics.newMeter(ObserverWrapper.class, this.appName + "_" + obsName
                + "@omid-aborts", "omid-aborts", TimeUnit.SECONDS));
        unknownAbortMeters.put(obsName, Metrics.newMeter(ObserverWrapper.class, this.appName + "_" + obsName
                + "@unknown-aborts", "unknown-aborts", TimeUnit.SECONDS));
        observerExecutionTimers.put(obsName,
                Metrics.newTimer(ObserverWrapper.class, this.appName + "_" + obsName + "-processing-time"));
    }

    public void notificationReceivedEvent() {
        notificationsMeter.mark();
    }

    public void observerInvocationEvent(String obsName) {
        observerInvocationMeters.get(obsName).mark();
    }

    public void observerCompletionEvent(String obsName) {
        observerCompletionMeters.get(obsName).mark();
    }

    public void observerAbortEvent(String obsName) {
        observerAbortMeters.get(obsName).mark();
    }

    public void omidAbortEvent(String obsName) {
        omidAbortMeters.get(obsName).mark();
    }

    public void unknownAbortEvent(String obsName) {
        unknownAbortMeters.get(obsName).mark();
    }

    public TimerContext startObserverInvocation(String obsName) {
        return observerExecutionTimers.get(obsName).time();
    }
}
