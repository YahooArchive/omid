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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.tso.messages.MinimumTimestamp;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;

/**
 * ChannelHandler for the TSO Server - sends oldest timestamp to HBase compaction coproc for cleanup
 *
 */
public class CompacterHandler extends SimpleChannelHandler implements EventHandler<CompacterHandler.CompactionEvent> {

    private final ChannelGroup channelGroup;
    private final ScheduledExecutorService executor;

    volatile long largestDeletedTimestamp = 0;

    public CompacterHandler(ChannelGroup channelGroup) {
        this.channelGroup = channelGroup;
        this.executor = Executors.newScheduledThreadPool(4, 
                                                         new ThreadFactoryBuilder().setNameFormat("compacter-executor-%d").build());
        this.executor.scheduleWithFixedDelay(new Notifier(channelGroup), 1, 1, TimeUnit.SECONDS);
    }

    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        //      System.out.println("New connection");
        channelGroup.add(ctx.getChannel());
    }

    private class Notifier implements Runnable {
        private final ChannelGroup channelGroup;
      
        public Notifier(ChannelGroup channelGroup) {
            this.channelGroup = channelGroup;
        }
        @Override
        public void run() {
            long timestamp = largestDeletedTimestamp;
            //         System.out.println("sending " + timestamp);
            channelGroup.write(new MinimumTimestamp(timestamp));
        }
    }

    public void stop() {
        this.executor.shutdown();
    }

    @Override
    public void onEvent(final CompactionEvent event, final long sequence, final boolean endOfBatch)
        throws Exception
    {
        largestDeletedTimestamp = event.getLargestDeletedTimestamp();
    }

    public final static class CompactionEvent {
        private long largestDeletedTimestamp = 0;

        long getLargestDeletedTimestamp() { return largestDeletedTimestamp; }
        CompactionEvent setLargestDeletedTimestamp(long largestDeletedTimestamp) {
            this.largestDeletedTimestamp = largestDeletedTimestamp;
            return this;
        }

        public final static EventFactory<CompactionEvent> EVENT_FACTORY
            = new EventFactory<CompactionEvent>()
        {
            public CompactionEvent newInstance()
            {
                return new CompactionEvent();
            }
        };
    }
}
