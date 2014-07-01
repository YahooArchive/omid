package com.yahoo.omid.tso;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockPanicker implements Panicker {
    private static final Logger LOG = LoggerFactory.getLogger(MockPanicker.class);

    @Override
    public void panic(String reason, Throwable cause) {
        LOG.error("PANICKING: {}", reason, cause);
    }
}
