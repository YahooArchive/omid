package com.yahoo.omid.client;

import com.yahoo.omid.client.TSOClient.ConnectedState;
import com.yahoo.omid.util.StateMachine.FsmImpl;

public class TSOClientAccessor {

    public static void closeChannel(TSOClient tsoClient) throws InterruptedException {
        FsmImpl fsm = (FsmImpl) tsoClient.fsm;
        ConnectedState connectedState = (ConnectedState) fsm.getState();
        connectedState.channel.close().await();
    }
}
