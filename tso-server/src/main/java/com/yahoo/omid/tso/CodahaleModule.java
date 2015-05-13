/**
 * Copyright 2011-2015 Yahoo Inc.
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

import static com.yahoo.omid.metrics.CodahaleMetricsProvider.CODAHALE_METRICS_CONFIG_PATTERN;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.yahoo.omid.metrics.CodahaleMetricsConfig;
import com.yahoo.omid.metrics.CodahaleMetricsConfig.Reporter;
import com.yahoo.omid.metrics.CodahaleMetricsProvider;
import com.yahoo.omid.metrics.MetricsRegistry;

public class CodahaleModule extends AbstractModule {

    private static final Logger LOG = LoggerFactory.getLogger(CodahaleModule.class);

    private final TSOServerCommandLineConfig config;
    private final CodahaleMetricsConfig codahaleConfig;

    public CodahaleModule(TSOServerCommandLineConfig config, CodahaleMetricsConfig codahaleConfig) {
        this.config = config;
        this.codahaleConfig = codahaleConfig;
    }

    @Override
    protected void configure() {

    }

    @Provides @Singleton
    MetricsRegistry provideMetricsRegistry() {
        for (String metricConfig : config.getMetricsConfigs()) {
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
