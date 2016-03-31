/**
 * Copyright 2011-2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.omid.tso;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.yahoo.omid.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.yahoo.omid.metrics.MetricsUtils.name;

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
        if (flag) {
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
