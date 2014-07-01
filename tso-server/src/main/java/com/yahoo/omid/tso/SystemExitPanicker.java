package com.yahoo.omid.tso;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemExitPanicker implements Panicker {
    private static final Logger LOG = LoggerFactory.getLogger(SystemExitPanicker.class);
    private static final int PANIC_EXIT_CODE = 123;

    @Override
    public void panic(String reason, Throwable cause) {
        LOG.error("PANICKING: {}", reason, cause);
        System.exit(PANIC_EXIT_CODE);
    }
}
