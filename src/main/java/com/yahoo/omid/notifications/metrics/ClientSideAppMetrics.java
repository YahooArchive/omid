package com.yahoo.omid.notifications.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.yahoo.omid.notifications.client.NotificationManager;
import com.yahoo.omid.notifications.client.ObserverWrapper;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.reporting.ConsoleReporter;

public class ClientSideAppMetrics {

    private static Logger logger = Logger.getLogger(ClientSideAppMetrics.class);

    private String appName;
    
    private Meter notificationsMeter;
    
    private Map<String, Meter> observerInvocationMeters = new ConcurrentHashMap<String, Meter>(); // This is redundant with the counter offered by observerExexutionTimers
    private Map<String, Meter> observerCompletionMeters = new ConcurrentHashMap<String, Meter>();
    private Map<String, Meter> observerAbortMeters = new ConcurrentHashMap<String, Meter>();
    private Map<String, Meter> omidAbortMeters = new ConcurrentHashMap<String, Meter>();
    private Map<String, Meter> unknownAbortMeters = new ConcurrentHashMap<String, Meter>();
    private Map<String, Timer> observerExecutionTimers = new ConcurrentHashMap<String, Timer>();
    
    public ClientSideAppMetrics(String appName) {
        this.appName = appName;
        long period = Long.valueOf(10);
        TimeUnit timeUnit = TimeUnit.valueOf("SECONDS");
        logger.info("Reporting metrics on the console with frequency of " + period + timeUnit.name());
        ConsoleReporter.enable(period, timeUnit);
        
        notificationsMeter = Metrics.newMeter(NotificationManager.class, this.appName + "@notifications-received", "notifications", TimeUnit.SECONDS);
    }

    public void addObserver(String obsName) {
        observerInvocationMeters.put(obsName, Metrics.newMeter(ObserverWrapper.class, this.appName + "/" + obsName + "@invocations", "invocations", TimeUnit.SECONDS));
        observerCompletionMeters.put(obsName, Metrics.newMeter(ObserverWrapper.class, this.appName + "/" + obsName + "@completions", "completions", TimeUnit.SECONDS));
        observerAbortMeters.put(obsName, Metrics.newMeter(ObserverWrapper.class, this.appName + "/" + obsName + "@aborts", "aborts", TimeUnit.SECONDS));
        omidAbortMeters.put(obsName, Metrics.newMeter(ObserverWrapper.class, this.appName + "/" + obsName + "@omid-aborts", "omid-aborts", TimeUnit.SECONDS));
        unknownAbortMeters.put(obsName, Metrics.newMeter(ObserverWrapper.class, this.appName + "/" + obsName + "@unknown-aborts", "unknown-aborts", TimeUnit.SECONDS));
        observerExecutionTimers.put(obsName, Metrics.newTimer(ObserverWrapper.class, this.appName + "/" + obsName + "-processing-time"));
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
