package com.yahoo.omid.tsoclient;

import com.yahoo.omid.tsoclient.TSOClientImpl.ConnectedState;
import com.yahoo.statemachine.StateMachine.FsmImpl;

public class TSOClientAccessor {

    public static void closeChannel(TSOClient tsoClient) throws InterruptedException {
        TSOClientImpl clientimpl = (TSOClientImpl) tsoClient;
        FsmImpl fsm = (FsmImpl) clientimpl.fsm;
        ConnectedState connectedState = (ConnectedState) fsm.getState();
        connectedState.channel.close().await();
    }
}
