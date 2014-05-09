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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.proto.TSOProto;
import com.yahoo.omid.proto.TSOProto.Response;

/**
 * Communication endpoint for TSO clients.
 *
 */
public class TSOClientOneShot {
    private static final Logger LOG = LoggerFactory.getLogger(TSOClientOneShot.class);

    private final String host;
    private final int port;
    
      
    public TSOClientOneShot(String host, int port) {
        
        this.host = host;
        this.port = port;
        
    }

    public TSOProto.Response makeRequest(TSOProto.Request request) throws InterruptedException, ExecutionException {
        // Start client with Nb of active threads = 3 as maximum.
        ChannelFactory factory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setNameFormat("tsoclient-boss-%d").build()),
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setNameFormat("tsoclient-worker-%d").build()), 3);
        // Create the bootstrap
        ClientBootstrap bootstrap = new ClientBootstrap(factory);

        InetSocketAddress addr = new InetSocketAddress(host, port);

        ChannelPipeline pipeline = bootstrap.getPipeline();
        pipeline.addLast("lengthbaseddecoder",
                         new LengthFieldBasedFrameDecoder(8*1024, 0, 4, 0, 4));
        pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
        pipeline.addLast("protobufdecoder",
                         new ProtobufDecoder(TSOProto.Response.getDefaultInstance()));
        pipeline.addLast("protobufencoder", new ProtobufEncoder());

        Handler handler = new Handler();
        pipeline.addLast("handler", handler);

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("connectTimeoutMillis", 100);
        
        ChannelFuture channelFuture = bootstrap.connect(addr).await();
        Channel channel = channelFuture.getChannel();
        
        channel.write(request);
        TSOProto.Response response = handler.getResponse();
        
        channel.close();
        return response;
    }

    private class Handler extends SimpleChannelHandler {
        
        private SettableFuture<TSOProto.Response> future = SettableFuture.<TSOProto.Response>create();

        public Response getResponse() throws InterruptedException, ExecutionException {
            return future.get();
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            if (e.getMessage() instanceof TSOProto.Response) {
                future.set((TSOProto.Response) e.getMessage());
            } else {
                LOG.warn("Received unknown message", e.getMessage());
            }
        }
    }
}
