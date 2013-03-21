package com.yahoo.omid.notifications.metrics;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.CsvReporter;

public class MetricsUtils {

    static Logger logger = LoggerFactory.getLogger(MetricsUtils.class);
    static final Pattern METRICS_CONFIG_PATTERN = Pattern
            .compile("(csv:.+|console):(\\d+):(DAYS|HOURS|MICROSECONDS|MILLISECONDS|MINUTES|NANOSECONDS|SECONDS)");

    public static void initMetrics(String metricsConfig) {
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
    }
}
