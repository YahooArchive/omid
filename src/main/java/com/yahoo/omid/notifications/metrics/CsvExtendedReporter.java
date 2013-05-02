package com.yahoo.omid.notifications.metrics;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.CsvReporter;
import com.yammer.metrics.stats.Snapshot;
/**
 * A reporter which periodically appends data from each metric to a metric-specific CSV file in
 * an output directory.
 * 
 * For timers it also prints meter information
 */
public class CsvExtendedReporter extends CsvReporter {

    /**
     * Enables the CSV extended reporter for the default metrics registry, and causes it to write to files in
     * {@code outputDir} with the specified period.
     *
     * @param outputDir    the directory in which {@code .csv} files will be created
     * @param period       the period between successive outputs
     * @param unit         the time unit of {@code period}
     */
    public static void enable(File outputDir, long period, TimeUnit unit) {
        enable(Metrics.defaultRegistry(), outputDir, period, unit);
    }

    /**
     * Enables the CSV reporter for the given metrics registry, and causes it to write to files in
     * {@code outputDir} with the specified period.
     *
     * @param metricsRegistry the metrics registry
     * @param outputDir       the directory in which {@code .csv} files will be created
     * @param period          the period between successive outputs
     * @param unit            the time unit of {@code period}
     */
    public static void enable(MetricsRegistry metricsRegistry, File outputDir, long period, TimeUnit unit) {
        final CsvReporter reporter = new CsvExtendedReporter(metricsRegistry, outputDir);
        reporter.start(period, unit);
    }

    public CsvExtendedReporter(MetricsRegistry metricsRegistry, File outputDir) {
        super(metricsRegistry, outputDir);
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Context context) throws IOException {
        final PrintStream stream = context.getStream(
                "# time,min,max,mean,median,stddev,95%,99%,99.9%,count,1 min rate,mean rate,5 min rate,15 min rate");
        final Snapshot snapshot = timer.getSnapshot();
        stream.append(new StringBuilder()
                              .append(timer.min()).append(',')
                              .append(timer.max()).append(',')
                              .append(timer.mean()).append(',')
                              .append(snapshot.getMedian()).append(',')
                              .append(timer.stdDev()).append(',')
                              .append(snapshot.get95thPercentile()).append(',')
                              .append(snapshot.get99thPercentile()).append(',')
                              .append(snapshot.get999thPercentile()).append(',')
                              .append(timer.count()).append(',')
                              .append(timer.oneMinuteRate()).append(',')
                              .append(timer.meanRate()).append(',')
                              .append(timer.fiveMinuteRate()).append(',')
                              .append(timer.fifteenMinuteRate())
                              .toString())
            .println();
        stream.flush();
    }
}
