package com.yahoo.omid.metrics;

import java.util.concurrent.TimeUnit;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.Inject;

@Singleton
public abstract class AbstractMetricsConfig {

    private static final int DEFAULT_OUTPUT_FREQ_IN_SECS = 60;

    private static final String OUTPUT_FREQ_KEY = "metrics.output.frequency.secs";
    private static final String OUTPUT_FREQ_TIME_UNIT_KEY = "metrics.output.frequency.time.unit";

    private int outputFreq = DEFAULT_OUTPUT_FREQ_IN_SECS;
    private static final TimeUnit DEFAULT_OUTPUT_FREQ_TIME_UNIT = TimeUnit.SECONDS;

    private TimeUnit outputFreqTimeUnit = DEFAULT_OUTPUT_FREQ_TIME_UNIT;

    public int getOutputFreq() {
        return outputFreq;
    }

    @Inject(optional = true)
    public void setOutputFreq(@Named(OUTPUT_FREQ_KEY) int outputFreq) {
        this.outputFreq = outputFreq;
    }

    public TimeUnit getOutputFreqTimeUnit() {
        return outputFreqTimeUnit;
    }

    @Inject(optional = true)
    public void setOutputFreqTimeUnit(@Named(OUTPUT_FREQ_TIME_UNIT_KEY) TimeUnit outputFreqTimeUnit) {
        this.outputFreqTimeUnit = outputFreqTimeUnit;
    }

}
