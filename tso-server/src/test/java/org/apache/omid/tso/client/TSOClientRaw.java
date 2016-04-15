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
package org.apache.omid.tso.client;

import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.omid.proto.TSOProto;
import org.apache.omid.proto.TSOProto.Response;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Raw client for communicating with tso server directly with protobuf messages
 */
public class TSOClientRaw {

    private static final Logger LOG = LoggerFactory.getLogger(TSOClientRaw.class);

    private final BlockingQueue<SettableFuture<Response>> responseQueue
            = new ArrayBlockingQueue<SettableFuture<Response>>(5);
    private final Channel channel;

    public TSOClientRaw(String host, int port) throws InterruptedException, ExecutionException {
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
                new LengthFieldBasedFrameDecoder(8 * 1024, 0, 4, 0, 4));
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
        channel = channelFuture.getChannel();
    }

    public void write(TSOProto.Request request) {
        channel.write(request);
    }

    public Future<Response> getResponse() throws InterruptedException {
        SettableFuture<Response> future = SettableFuture.<Response>create();
        responseQueue.put(future);
        return future;
    }

    public void close() throws InterruptedException {
        responseQueue.put(SettableFuture.<Response>create());
        channel.close();
    }

    private class Handler extends SimpleChannelHandler {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            LOG.info("Message received", e);
            if (e.getMessage() instanceof Response) {
                Response resp = (Response) e.getMessage();
                try {
                    SettableFuture<Response> future = responseQueue.take();
                    future.set(resp);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted in handler", ie);
                }
            } else {
                LOG.warn("Received unknown message", e.getMessage());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            LOG.info("Exception received", e.getCause());
            try {
                SettableFuture<Response> future = responseQueue.take();
                future.setException(e.getCause());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted handling exception", ie);
            }
        }

        @Override
        public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e)
                throws Exception {
            LOG.info("Disconnected");
            try {
                SettableFuture<Response> future = responseQueue.take();
                future.setException(new ConnectionException());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted handling exception", ie);
            }
        }
    }
}
