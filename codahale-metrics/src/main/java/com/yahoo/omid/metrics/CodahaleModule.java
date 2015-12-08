package com.yahoo.omid.metrics;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.metrics.CodahaleMetricsConfig.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static com.yahoo.omid.metrics.CodahaleMetricsProvider.CODAHALE_METRICS_CONFIG_PATTERN;

public class CodahaleModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(CodahaleModule.class);

    private final List<String> metricsConfigs;
    private final CodahaleMetricsConfig codahaleConfig = new CodahaleMetricsConfig();

    public CodahaleModule(List<String> config) {
        this.metricsConfigs = config;
    }

    @Override
    protected void configure() {

    }

    @Provides @Singleton
    MetricsRegistry provideMetricsRegistry() {
        for (String metricConfig : metricsConfigs) {
            Matcher matcher = CODAHALE_METRICS_CONFIG_PATTERN.matcher(metricConfig);
            if (matcher.matches()) {

                String reporter = matcher.group(1);
                String reporterConfig = matcher.group(2);
                codahaleConfig.setOutputFreq(Integer.valueOf(matcher.group(3)));
                codahaleConfig.setOutputFreqTimeUnit(TimeUnit.valueOf(matcher.group(4)));

                switch(reporter) {
                case "csv":
                    codahaleConfig.addReporter(Reporter.CSV);
                    codahaleConfig.setCSVDir(reporterConfig);
                    break;
                case "slf4j":
                    codahaleConfig.addReporter(Reporter.SLF4J);
                    codahaleConfig.setSlf4jLogger(reporterConfig);
                    break;
                case "graphite":
                    codahaleConfig.addReporter(Reporter.GRAPHITE);
                    codahaleConfig.setGraphiteHostConfig(reporterConfig);
                    break;
                case "console":
                    codahaleConfig.addReporter(Reporter.CONSOLE);
                    break;
                default:
                    LOG.warn("Reporter {} unknown", reporter);
                    break;
                }
            } else {
                LOG.error("Pattern {} not recognized", metricConfig);
            }
        }
        CodahaleMetricsProvider provider = new CodahaleMetricsProvider(codahaleConfig);
        provider.startMetrics();
        return provider;
    }

}
