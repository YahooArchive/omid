package com.yahoo.omid.examples.notifications;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.core.Timer;

public class LatencyUtils {
    static Logger LOG = LoggerFactory.getLogger(LatencyUtils.class);

    public static void measureLatencies(Timer fromInjector, Timer fromLastStage, KeyValue timestamps) {
        if (timestamps == null) {
            LOG.error("No timestamps");
            return;
        }

        String[] strings = Bytes.toString(timestamps.getValue()).split(";");
        long firstTS = Long.parseLong(strings[0]);
        long lastTS = Long.parseLong(strings[strings.length - 1]);
        long current = System.currentTimeMillis();
        fromInjector.update(current - firstTS, TimeUnit.MILLISECONDS);
        fromLastStage.update(current - lastTS, TimeUnit.MILLISECONDS);
    }

    public static byte[] buildLatencyValue(KeyValue timestamps) {
        return Bytes.toBytes(Bytes.toString(timestamps.getValue()) + ";" + System.currentTimeMillis());
    }
}
