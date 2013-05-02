package com.yahoo.omid.client.metrics;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.tso.TSOHandler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class OmidClientMetrics {

    private static Logger logger = LoggerFactory.getLogger(OmidClientMetrics.class);

    public enum Timers {
        COMMIT, ABORT, QUERY, BEGIN, GET, PUT, DELETE, SCANNER, NEXT, CLEANUP, FLUSH_COMMITS, FILTER_READS
    };

    public enum Meters {
        EXTRA_VERSIONS
    };

    private Map<Timers, Timer> timers;
    private Map<Meters, Meter> meters;

    public OmidClientMetrics() {
        timers = new EnumMap<OmidClientMetrics.Timers, Timer>(Timers.class);
        for (Timers t : Timers.values()) {
            timers.put(t, Metrics.defaultRegistry()
                    .newTimer(TSOHandler.class, "omidClient@timer-" + t, "omidClient"));
        }
        meters = new EnumMap<OmidClientMetrics.Meters, Meter>(Meters.class);
        for (Meters m : Meters.values()) {
            meters.put(m, Metrics.defaultRegistry()
                    .newMeter(TSOHandler.class, "omidClient@meter-" + m, "omidClient", TimeUnit.SECONDS));
        }
    }

    public TimerContext startTimer(Timers t) {
        return timers.get(t).time();
    }

    public void count(Meters m) {
        meters.get(m).mark();
    }

    public void count(long count, Meters m) {
        meters.get(m).mark(count);
    }
}
