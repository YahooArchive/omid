package com.yahoo.omid.tso;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Implements the management of the state of the TSO
 */
public class TSOStateManagerImpl implements TSOStateManager {

    private static final Logger LOG = LoggerFactory.getLogger(TSOStateManagerImpl.class);

    private List<StateObserver> stateObservers = new ArrayList<>();

    private TSOState state;

    private TimestampOracle timestampOracle;

    @Inject
    public TSOStateManagerImpl(TimestampOracle timestampOracle) {
        this.timestampOracle = timestampOracle;
    }

    @Override
    public synchronized void register(StateObserver newObserver) {
        Preconditions.checkNotNull(newObserver, "Trying to register a null observer");
        if (!stateObservers.contains(newObserver)) {
            stateObservers.add(newObserver);
        }
    }

    @Override
    public synchronized void unregister(StateObserver observer) {
        stateObservers.remove(observer);
    }

    @Override
    public synchronized TSOState reset() throws IOException {
        LOG.info("Reseting the TSO Server state...");
        // The timestamp oracle dictates the new state
        timestampOracle.initialize();
        long lowWatermark = timestampOracle.getLast();
        // In this implementation the epoch == low watermark
        long epoch = lowWatermark;
        state = new TSOState(lowWatermark, epoch);

        // Then, notify registered observers about the new state
        for (StateObserver stateObserver : stateObservers) {
            stateObserver.update(state);
        }
        LOG.info("New TSO Server state {}", state);
        return state;
    }

}
