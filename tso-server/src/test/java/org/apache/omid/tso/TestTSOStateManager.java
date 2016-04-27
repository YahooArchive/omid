/*
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
package org.apache.omid.tso;

import org.apache.omid.tso.TSOStateManager.StateObserver;
import org.apache.omid.tso.TSOStateManager.TSOState;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class TestTSOStateManager {

    private static final long INITIAL_STATE_VALUE = 1L;
    private static final long NEW_STATE_VALUE = 1000;

    // Mocks
    private TimestampOracle timestampOracle = mock(TimestampOracle.class);

    // Component under test
    private TSOStateManager stateManager = new TSOStateManagerImpl(timestampOracle);

    @BeforeMethod
    public void beforeMethod() {
        // Initialize the state with the one reported by the Timestamp Oracle
        when(timestampOracle.getLast()).thenReturn(INITIAL_STATE_VALUE);
    }

    @Test
    public void testTSOServerStateInitialization() throws Exception {

        // Reset the state and check we get the initial state values
        TSOState initialState = stateManager.initialize();
        assertEquals(initialState.getLowWatermark(), INITIAL_STATE_VALUE);
        assertEquals(initialState.getEpoch(), INITIAL_STATE_VALUE);
        assertTrue("In this implementation low watermark should be equal to epoch",
                   initialState.getLowWatermark() == initialState.getEpoch());

        // Then, simulate a change in the state returned by the Timestamp Oracle...
        when(timestampOracle.getLast()).thenReturn(NEW_STATE_VALUE);
        // ... and again, reset the state and check we get the new values
        TSOState secondState = stateManager.initialize();
        assertEquals(secondState.getLowWatermark(), NEW_STATE_VALUE);
        assertEquals(secondState.getEpoch(), NEW_STATE_VALUE);
        assertTrue("In this implementation low watermark should be equal to epoch",
                   secondState.getLowWatermark() == secondState.getEpoch());

    }

    @Test
    public void testObserverRegistrationAndDeregistrationForStateChanges() throws Exception {

        // Register observer 1 for receiving state changes
        StateObserver observer1 = spy(new DummyObserver());
        stateManager.register(observer1);

        // Reset the state to trigger observer notifications
        TSOState state = stateManager.initialize();

        // Check observer 1 was notified with the corresponding state
        verify(observer1, timeout(100).times(1)).update(eq(state));

        // Register observer 1 for receiving state changes
        StateObserver observer2 = spy(new DummyObserver());
        stateManager.register(observer2);

        // Again, reset the state to trigger observer notifications
        state = stateManager.initialize();

        // Check both observers were notified with the corresponding state
        verify(observer1, timeout(100).times(1)).update(eq(state));
        verify(observer2, timeout(100).times(1)).update(eq(state));

        // De-register observer 1
        stateManager.unregister(observer1);

        // Again, reset the state to trigger observer notifications
        state = stateManager.initialize();

        // Check only observer 2 was notified
        verify(observer1, timeout(100).times(0)).update(eq(state));
        verify(observer2, timeout(100).times(1)).update(eq(state));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Helper classes
    // ----------------------------------------------------------------------------------------------------------------

    private class DummyObserver implements StateObserver {

        @Override
        public void update(TSOState state) throws IOException {
        }

    }

}
