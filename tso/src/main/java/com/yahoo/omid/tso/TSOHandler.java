/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso;

import java.net.InetSocketAddress;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;

import com.lmax.disruptor.RingBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ChannelHandler for the TSO Server.
 * <p>
 * 
 * Incoming requests are processed in this class and by accessing a shared data structure, {@link TSOState}
 * 
 */
public class TSOHandler extends SimpleChannelHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TSOHandler.class);

    /**
     * Channel Group
     */
    private ChannelGroup channelGroup = null;
    private final RequestProcessor requestProcessor;

    /**
     * Constructor
     * 
     * @param channelGroup
     */
    public TSOHandler(ChannelGroup channelGroup,
                      RequestProcessor requestProcessor) {
        this.channelGroup = channelGroup;
        this.requestProcessor = requestProcessor;
     }

    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channelGroup.add(ctx.getChannel());
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channelGroup.remove(ctx.getChannel());
    }

    /**
     * Handle receieved messages
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        Object msg = e.getMessage();

        //long seq = ringBuffer.next();
        //RequestProcessor.RequestEvent event = ringBuffer.get(seq);
        //event.setContext(ctx);
        //event.setMessage(msg);

        //ringBuffer.publish(seq);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        LOG.warn("TSOHandler: Unexpected exception from downstream.", e.getCause());
        Channels.close(e.getChannel());
    }
}
