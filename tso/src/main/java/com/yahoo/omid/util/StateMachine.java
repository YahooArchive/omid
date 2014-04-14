package com.yahoo.omid.util;

import com.google.common.util.concurrent.AbstractFuture;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;

import java.util.Queue;
import java.util.ArrayDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateMachine {
    static final Logger LOG = LoggerFactory.getLogger(StateMachine.class);

    public interface State {
        State handleEvent(Fsm fsm, Event e);
    }

    public interface Event { }
    
    public interface Fsm {
        void setInitState(State initState);
        void sendEvent(Event e);
        void sendEvent(Event e, long delay, TimeUnit unit);
        void deferUserEvent(UserEvent e);
    }

    public static class FsmImpl implements Fsm {
        ScheduledExecutorService executor;
        private State state;
        private Queue<UserEvent> deferred;

        public FsmImpl(ScheduledExecutorService executor) {
            this.executor = executor;
            state = null;
            deferred = new ArrayDeque<UserEvent>();
        }

        private void errorUserEvents(Throwable t) {
            Queue<UserEvent> oldDeferred = deferred;
            deferred = new ArrayDeque<UserEvent>();

            for (UserEvent e : oldDeferred) {
                e.error(new IllegalStateException(t));
            }
        }

        @Override
        public void setInitState(State initState) {
            assert (state == null);
            state = initState;
        }

        void setState(final State curState, final State newState) {
            if (curState != state) {
                LOG.error("Tried to transition from {} to {}, but current state is {}",
                          new Object[] { state, newState, curState });
                throw new IllegalArgumentException();
            }
            state = newState;

            Queue<UserEvent> oldDeferred = deferred;
            deferred = new ArrayDeque<UserEvent>();

            for (UserEvent e : oldDeferred) {
                sendEvent(e);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("State transition {} -> {}",
                          new Object[] { curState, newState });
            }
        }

        @Override
        public void sendEvent(final Event e) {
            executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Received event {} in state {}",
                                      new Object[] { e.getClass().getSimpleName(), state });
                        }
                        try {
                            State newState = state.handleEvent(FsmImpl.this, e);
                            if (newState != state) {
                                setState(state, newState);
                            }
                        } catch (Throwable t) {
                            LOG.error("Caught throwable while handling event", t);
                            errorUserEvents(t);
                        }
                    }
                });
        }

        @Override
        public void sendEvent(final Event e, final long delay, final TimeUnit unit) {
            executor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Received delayed event {} in state {}",
                                      new Object[] { e.getClass().getSimpleName(), state });
                        }
                        try {
                            State newState = state.handleEvent(FsmImpl.this, e);
                            if (newState != state) {
                                setState(state, newState);
                            }
                        } catch (Throwable t) {
                            LOG.error("Caught throwable while handling event", t);
                            errorUserEvents(t);
                        }
                    }
                }, delay, unit);
        }

        @Override
        public void deferUserEvent(UserEvent e) {
            deferred.add(e);
        }

        // TODO add cleanup
    }

    public static class UserEvent<T> extends AbstractFuture<T> implements Event {
        public void error(Throwable exception) {
            setException(exception);
        }

        public void success(T value) {
            set(value);
        }
    }
}
