package com.yahoo.omid.metrics;

public interface Timer extends Metric {

    public void start();

    public void stop();

    public void update(long durationInNs);

}
