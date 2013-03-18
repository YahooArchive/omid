package com.yahoo.omid.notifications.metrics;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.yahoo.omid.notifications.client.NotificationManager;
import com.yahoo.omid.notifications.client.ObserverWrapper;
import com.yahoo.omid.notifications.conf.ClientConfiguration;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.CsvReporter;

public class ClientSideAppMetrics {

    private static org.slf4j.Logger logger = LoggerFactory.getLogger(ClientSideAppMetrics.class);

    static final Pattern METRICS_CONFIG_PATTERN = Pattern
            .compile("(csv:.+|console):(\\d+):(DAYS|HOURS|MICROSECONDS|MILLISECONDS|MINUTES|NANOSECONDS|SECONDS)");
    
    private String appName;
    
    private Meter notificationsMeter;
    
    private Map<String, Meter> observerInvocationMeters = new ConcurrentHashMap<String, Meter>(); // This is redundant with the counter offered by observerExexutionTimers
    private Map<String, Meter> observerCompletionMeters = new ConcurrentHashMap<String, Meter>();
    private Map<String, Meter> observerAbortMeters = new ConcurrentHashMap<String, Meter>();
    private Map<String, Timer> observerExecutionTimers = new ConcurrentHashMap<String, Timer>();
    
    public ClientSideAppMetrics(String appName, ClientConfiguration conf) {
        this.appName = appName;
        String metricsConfig= conf.getString("omid.metrics");
        logger.info("metrics= {}" , metricsConfig);
        Matcher matcher = METRICS_CONFIG_PATTERN.matcher(metricsConfig);
        if (!matcher.matches()) {
            logger.error(
                    "Invalid metrics configuration [{}]. Metrics configuration must match the pattern [{}]. Metrics reporting disabled.",
                    metricsConfig, METRICS_CONFIG_PATTERN);
        } else {
        	String group1 = matcher.group(1);

            if (group1.startsWith("csv")) {
                String outputDir = group1.substring("csv:".length());
                long period = Long.valueOf(matcher.group(2));
                TimeUnit timeUnit = TimeUnit.valueOf(matcher.group(3));
                if (!(new File(outputDir).exists())) {
                	logger.error("output dir {} does not exist", outputDir);
                }
                logger.info("Reporting metrics through csv files in directory [{}] with frequency of [{}] [{}]",
                        new String[] { outputDir, String.valueOf(period), timeUnit.name() });
                CsvReporter.enable(new File(outputDir), period, timeUnit);
            } else {
                long period = Long.valueOf(matcher.group(2));
                TimeUnit timeUnit = TimeUnit.valueOf(matcher.group(3));
                logger.info("Reporting metrics on the console with frequency of [{}] [{}]",
                        new String[] { String.valueOf(period), timeUnit.name() });
                ConsoleReporter.enable(period, timeUnit);
            }
        }
        
        notificationsMeter = Metrics.defaultRegistry().newMeter(NotificationManager.class, this.appName + "@notifications-received", "notifications", TimeUnit.SECONDS);
    }

    public void addObserver(String obsName) {
        observerInvocationMeters.put(obsName, Metrics.defaultRegistry().newMeter(ObserverWrapper.class, this.appName + "_" + obsName + "@invocations", "invocations", TimeUnit.SECONDS));
        observerCompletionMeters.put(obsName, Metrics.defaultRegistry().newMeter(ObserverWrapper.class, this.appName + "_" + obsName + "@completions", "completions", TimeUnit.SECONDS));
        observerAbortMeters.put(obsName, Metrics.defaultRegistry().newMeter(ObserverWrapper.class, this.appName + "_" + obsName + "@aborts", "aborts", TimeUnit.SECONDS));
        observerExecutionTimers.put(obsName, Metrics.defaultRegistry().newTimer(ObserverWrapper.class, this.appName + "_" + obsName + "-processing-time"));
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

    public TimerContext startObserverInvocation(String obsName) {
        return observerExecutionTimers.get(obsName).time();
    }
}
