package com.yahoo.omid.tso;

import org.jboss.netty.channel.ChannelHandlerContext;
import com.lmax.disruptor.EventFactory;

public final class TSOEvent {
    private ChannelHandlerContext ctx = null;
    private Object msg = null;

    ChannelHandlerContext getContext() {
        return ctx;
    }

    void setContext(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    Object getMessage() {
        return msg;
    }

    void setMessage(Object msg) {
        this.msg = msg;
    }

    public final static EventFactory<TSOEvent> EVENT_FACTORY = new EventFactory<TSOEvent>()
    {
        public TSOEvent newInstance()
        {
            return new TSOEvent();
        }
    };
}
