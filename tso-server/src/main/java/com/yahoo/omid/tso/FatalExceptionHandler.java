package com.yahoo.omid.tso;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FatalExceptionHandler implements ExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FatalExceptionHandler.class);

    Panicker panicker;

    FatalExceptionHandler(Panicker panicker) {
        this.panicker = panicker;
    }

    @Override
    public void handleEventException(Throwable ex,
                                     long sequence,
                                     Object event) {
        LOG.error("Uncaught exception throws for sequence {}, event {}",
                  new Object[] { sequence, event, ex });
        panicker.panic("Uncaught exception in disruptor thread", ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        LOG.warn("Uncaught exception shutting down", ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        panicker.panic("Uncaught exception starting up", ex);
    }
}
