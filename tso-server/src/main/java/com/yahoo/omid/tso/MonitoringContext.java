package com.yahoo.omid.tso;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.yahoo.omid.metrics.MetricsRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import static com.codahale.metrics.MetricRegistry.name;

@NotThreadSafe
public class MonitoringContext {
    private static final Logger LOG = LoggerFactory.getLogger(MonitoringContext.class);
    private volatile boolean flag;
    private Map<String, Long> elapsedTimeMsMap = new HashMap<>();
    private Map<String, Stopwatch> timers = new ConcurrentHashMap<>();
    private MetricsRegistry metrics;

    public MonitoringContext(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

    public void timerStart(String name) {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        timers.put(name, stopwatch);
    }

    public void timerStop(String name) {
        if(flag){
            LOG.warn("timerStop({}) called after publish. Measurement was ignored. {}", name, Throwables.getStackTraceAsString(new Exception()));
            return;
        }
        Stopwatch activeStopwatch = timers.get(name);
        if (activeStopwatch == null) {
            throw new IllegalStateException(
                String.format("There is no %s timer in the %s monitoring context.", name, this));
        }
        activeStopwatch.stop();
        elapsedTimeMsMap.put(name, activeStopwatch.elapsedTime(TimeUnit.NANOSECONDS));
        timers.remove(name);
    }

    public void publish() {
        flag = true;
        for (String name : elapsedTimeMsMap.keySet()) {
            Long durationInNs = elapsedTimeMsMap.get(name);
            metrics.timer(name("tso", name)).update(durationInNs);
        }
    }
}
