package com.yahoo.omid.metrics;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.yammer.metrics.reporting.ConsoleReporter;
//import com.yammer.metrics.reporting.GraphiteReporter;

/**
 * Parses metrics configuration and initializes metrics reporting.
 *
 */
public class MetricsUtils {

    static Logger logger = LoggerFactory.getLogger(MetricsUtils.class);
    static final Pattern METRICS_CONFIG_PATTERN = Pattern
            .compile("(csv:.+|console|graphite:.+:.+):(\\d+):(DAYS|HOURS|MICROSECONDS|MILLISECONDS|MINUTES|NANOSECONDS|SECONDS)");

    public static void initMetrics(String metricsConfig) {
        /*        if (metricsConfig == null || metricsConfig.equals("")) {
            metricsConfig = "console:1:MINUTES";
        }
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
                    logger.warn("Output dir for metrics {} does not exist! Creating...", outputDir);
                    boolean success = (new File(outputDir)).mkdirs();
                    if (!success) {
                        logger.error("Output dir for metrics {} cannot be created! NO metrics provided!", outputDir);
                        return;
                    }
                }
                logger.info("Reporting metrics through csv files in directory [{}] with frequency of [{}] [{}]",
                        new String[] { outputDir, String.valueOf(period), timeUnit.name() });
                CsvExtendedReporter.enable(new File(outputDir), period, timeUnit);
            } else if (group1.startsWith("graphite")) {
                long period = Long.valueOf(matcher.group(2));
                TimeUnit timeUnit = TimeUnit.valueOf(matcher.group(3));
                String parts[] = group1.split(":");
                assert (parts[0].equals("graphite"));
                String host = parts[1];
                Integer port = Integer.valueOf(parts[2]);
                logger.info("Reporting metrics to graphite {}:{} with frequency of [{}] [{}]",
                        new Object[] { host, port, String.valueOf(period), timeUnit.name() });
                GraphiteReporter.enable(period, timeUnit, host, port, "omid");
            } else {
                long period = Long.valueOf(matcher.group(2));
                TimeUnit timeUnit = TimeUnit.valueOf(matcher.group(3));
                logger.info("Reporting metrics on the console with frequency of [{}] [{}]",
                        new String[] { String.valueOf(period), timeUnit.name() });
                ConsoleReporter.enable(period, timeUnit);
            }
            }*/
    }
}
