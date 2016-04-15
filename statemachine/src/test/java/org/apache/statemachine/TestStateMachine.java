/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.statemachine;

import org.apache.statemachine.StateMachine.Fsm;
import org.apache.statemachine.StateMachine.FsmImpl;
import org.apache.statemachine.StateMachine.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

public class TestStateMachine {
    private final static Logger LOG = LoggerFactory.getLogger(TestStateMachine.class);

    static class TestEvent implements StateMachine.DeferrableEvent {
        CountDownLatch latch = new CountDownLatch(1);
        Throwable t = null;
        int i = 0;

        public void error(Throwable t) {
            this.t = t;
            latch.countDown();
        }

        public void success(int i) {
            this.i = i;
            latch.countDown();
        }

        public int get() throws InterruptedException, Throwable {
            latch.await();
            if (t != null) {
                throw t;
            }
            return i;
        }
    }

    static class CompletingState extends State {
        int completed = 0;

        CompletingState(Fsm fsm) {
            super(fsm);
        }

        public State handleEvent(TestEvent e) {
            e.success(completed++);
            return this;
        }
    }

    static class DeferringState extends State {
        int count = 0;

        DeferringState(Fsm fsm) {
            super(fsm);
        }

        public State handleEvent(TestEvent e) {
            if (count++ < 5) {
                fsm.deferEvent(e);
                return this;
            } else {
                fsm.deferEvent(e);
                return new CompletingState(fsm);
            }
        }
    }

    @Test(timeOut = 60000)
    public void testOrdering() throws Throwable {
        Fsm fsm = new FsmImpl(Executors.newSingleThreadScheduledExecutor());
        fsm.setInitState(new DeferringState(fsm));
        for (int i = 0; i < 10; i++) {
            fsm.sendEvent(new TestEvent());
        }
        TestEvent te = new TestEvent();
        fsm.sendEvent(te);
        Assert.assertEquals(10, te.get());
    }
}
