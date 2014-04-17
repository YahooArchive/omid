package com.yahoo.omid.metrics;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

/**
 * Parses metrics configuration and initializes metrics reporting.
 *
 */
public class MetricsUtils {

    static Logger logger = LoggerFactory.getLogger(MetricsUtils.class);
    static final Pattern METRICS_CONFIG_PATTERN = Pattern
            .compile("(csv|slf4j|console|graphite):(.+):(\\d+):(DAYS|HOURS|MICROSECONDS|MILLISECONDS|MINUTES|NANOSECONDS|SECONDS)");

    public static MetricRegistry initMetrics(List<String> metricsConfig) {
        MetricRegistry r = new MetricRegistry();
        for (String s : metricsConfig) {
            initForConfig(r, s);
        }
        return r;
    }

    private static void initForConfig(MetricRegistry metrics, String metricsConfig) {
        Matcher matcher = METRICS_CONFIG_PATTERN.matcher(metricsConfig);
        if (!matcher.matches()) {
            logger.error(
                    "Invalid metrics configuration [{}]. Metrics configuration must match the pattern [{}]."
                    + " Metrics reporting for this config disabled.",
                    metricsConfig, METRICS_CONFIG_PATTERN);
        } else {
            String type = matcher.group(1);
            String typeSpec = matcher.group(2);
            long period = Long.valueOf(matcher.group(3));
            TimeUnit timeUnit = TimeUnit.valueOf(matcher.group(4));

            if (type.equals("csv")) {
                File outputDir = new File(typeSpec);

                if (!outputDir.exists()) {
                    logger.warn("Output dir for metrics {} does not exist! Creating...", outputDir);
                    boolean success = outputDir.mkdirs();
                    if (!success) {
                        logger.error("Output dir for metrics {} cannot be created! NO metrics provided!", outputDir);
                        return;
                    }
                }

                final CsvReporter reporter = CsvReporter.forRegistry(metrics)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(outputDir);
                reporter.start(period, timeUnit);

                logger.info("Reporting metrics through csv files in directory [{}] with frequency of [{}] [{}]",
                            new Object[] { outputDir, String.valueOf(period), timeUnit.name() });
            } else if (type.equals("slf4j")) {
                final Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
                    .outputTo(LoggerFactory.getLogger(typeSpec))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
                reporter.start(period, timeUnit);
                logger.info("Reporting metrics on slf4j (logger:{}) with frequency of [{}] [{}]",
                            new Object[] { typeSpec, period, timeUnit.name() });
            } else if (type.equals("graphite")) {
                String parts[] = typeSpec.split(":");
                String host = parts[0];
                Integer port = Integer.valueOf(parts[1]);

                final Graphite graphite = new Graphite(new InetSocketAddress(host, port));
                final GraphiteReporter reporter = GraphiteReporter.forRegistry(metrics)
                    .prefixedWith("omid")
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    .build(graphite);
                reporter.start(period, timeUnit);
                logger.info("Reporting metrics to graphite {}:{} with frequency of [{}] [{}]",
                        new Object[] { host, port, period, timeUnit.name() });
            } else if (type.equals("console")) {
                final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
                reporter.start(period, timeUnit);

                logger.info("Reporting metrics on the console with frequency of [{}] [{}]",
                            period, timeUnit.name());
            }
        }
    }
}
