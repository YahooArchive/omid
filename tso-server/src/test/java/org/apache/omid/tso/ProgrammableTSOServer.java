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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.omid.proto.TSOProto;
import org.apache.omid.tso.ProgrammableTSOServer.Response.ResponseType;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executors;

/**
 * Used in tests. Allows to program the set of responses returned by a TSO
 */
public class ProgrammableTSOServer extends SimpleChannelHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ProgrammableTSOServer.class);

    private ChannelFactory factory;
    private ChannelGroup channelGroup;

    private Queue<Response> responseQueue = new LinkedList<>();

    @Inject
    public ProgrammableTSOServer(int port) {
        // Setup netty listener
        factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("boss-%d").build()), Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("worker-%d").build()), (Runtime.getRuntime().availableProcessors() * 2 + 1) * 2);

        // Create the global ChannelGroup
        channelGroup = new DefaultChannelGroup(ProgrammableTSOServer.class.getName());

        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        bootstrap.setPipelineFactory(new TSOChannelHandler.TSOPipelineFactory(this));

        // Add the parent channel to the group
        Channel channel = bootstrap.bind(new InetSocketAddress(port));
        channelGroup.add(channel);

        LOG.info("********** Dumb TSO Server running on port {} **********", port);
    }

    // ************************* Main interface for tests *********************

    /**
     * Allows to add response to the queue of responses
     *
     * @param r
     *            the response to add
     */
    public void queueResponse(Response r) {
        responseQueue.add(r);
    }

    /**
     * Removes all the current responses in the queue
     */
    public void cleanResponses() {
        responseQueue.clear();
    }

    // ******************** End of Main interface for tests *******************

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channelGroup.add(ctx.getChannel());
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channelGroup.remove(ctx.getChannel());
    }

    /**
     * Handle received messages
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        Object msg = e.getMessage();
        if (msg instanceof TSOProto.Request) {
            TSOProto.Request request = (TSOProto.Request) msg;
            Channel channel = ctx.getChannel();
            if (request.hasHandshakeRequest()) {
                checkHandshake(ctx, request.getHandshakeRequest());
                return;
            }
            if (!handshakeCompleted(ctx)) {
                LOG.info("handshake not completed");
                channel.close();
            }

            Response resp = responseQueue.poll();
            if (request.hasTimestampRequest()) {
                if (resp == null || resp.type != ResponseType.TIMESTAMP) {
                    throw new IllegalStateException("Expecting TS response to send but got " + resp);
                }
                TimestampResponse tsResp = (TimestampResponse) resp;
                sendTimestampResponse(tsResp.startTS, channel);
            } else if (request.hasCommitRequest()) {
                if (resp == null) {
                    throw new IllegalStateException("Expecting COMMIT response to send but got null");
                }
                switch (resp.type) {
                    case COMMIT:
                        CommitResponse commitResp = (CommitResponse) resp;
                        sendCommitResponse(commitResp.heuristicDecissionRequired,
                                commitResp.startTS,
                                commitResp.commitTS,
                                channel);
                        break;
                    case ABORT:
                        AbortResponse abortResp = (AbortResponse) resp;
                        sendAbortResponse(abortResp.startTS, channel);
                        break;
                    default:
                        throw new IllegalStateException("Expecting COMMIT response to send but got " + resp.type);
                }
            } else {
                LOG.error("Invalid request {}", request);
                ctx.getChannel().close();
            }
        } else {
            LOG.error("Unknown message type", msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        if (e.getCause() instanceof ClosedChannelException) {
            return;
        }
        LOG.warn("TSOHandler: Unexpected exception from downstream.", e.getCause());
        Channels.close(e.getChannel());
    }

    private void checkHandshake(final ChannelHandlerContext ctx, TSOProto.HandshakeRequest request) {
        TSOProto.HandshakeResponse.Builder response = TSOProto.HandshakeResponse.newBuilder();
        if (request.hasClientCapabilities()) {

            response.setClientCompatible(true).setServerCapabilities(TSOProto.Capabilities.newBuilder().build());
            TSOChannelContext tsoCtx = new TSOChannelContext();
            tsoCtx.setHandshakeComplete();
            ctx.setAttachment(tsoCtx);
        } else {
            response.setClientCompatible(false);
        }
        ctx.getChannel().write(TSOProto.Response.newBuilder().setHandshakeResponse(response.build()).build());
    }

    private boolean handshakeCompleted(ChannelHandlerContext ctx) {
        Object o = ctx.getAttachment();
        if (o instanceof TSOChannelContext) {
            TSOChannelContext tsoCtx = (TSOChannelContext) o;
            return tsoCtx.getHandshakeComplete();
        }
        return false;
    }

    private void sendTimestampResponse(long startTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.TimestampResponse.Builder respBuilder = TSOProto.TimestampResponse.newBuilder();
        respBuilder.setStartTimestamp(startTimestamp);
        builder.setTimestampResponse(respBuilder.build());
        c.write(builder.build());
    }

    private void sendCommitResponse(boolean makeHeuristicDecission, long startTimestamp, long commitTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        if (makeHeuristicDecission) { // If the commit is ambiguous is due to a new master TSO
            commitBuilder.setMakeHeuristicDecision(true);
        }
        commitBuilder.setAborted(false).setStartTimestamp(startTimestamp).setCommitTimestamp(commitTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.write(builder.build());
    }

    private void sendAbortResponse(long startTimestamp, Channel c) {
        TSOProto.Response.Builder builder = TSOProto.Response.newBuilder();
        TSOProto.CommitResponse.Builder commitBuilder = TSOProto.CommitResponse.newBuilder();
        commitBuilder.setAborted(true).setStartTimestamp(startTimestamp);
        builder.setCommitResponse(commitBuilder.build());
        c.write(builder.build());
    }

    private static class TSOChannelContext {
        boolean handshakeComplete;

        TSOChannelContext() {
            handshakeComplete = false;
        }

        boolean getHandshakeComplete() {
            return handshakeComplete;
        }

        void setHandshakeComplete() {
            handshakeComplete = true;
        }
    }

    public static class TimestampResponse extends Response {

        final long startTS;

        public TimestampResponse(long startTS) {
            super(ResponseType.TIMESTAMP);
            this.startTS = startTS;
        }

    }

    public static class CommitResponse extends Response {

        final boolean heuristicDecissionRequired;
        final long startTS;
        final long commitTS;

        public CommitResponse(boolean heuristicDecissionRequired, long startTS, long commitTS) {
            super(ResponseType.COMMIT);
            this.heuristicDecissionRequired = heuristicDecissionRequired;
            this.startTS = startTS;
            this.commitTS = commitTS;
        }

    }

    public static class AbortResponse extends Response {

        final long startTS;

        public AbortResponse(long startTS) {
            super(ResponseType.ABORT);
            this.startTS = startTS;
        }

    }

    abstract static class Response {

        enum ResponseType {
            TIMESTAMP, COMMIT, ABORT
        }

        final ResponseType type;

        public Response(ResponseType type) {
            this.type = type;
        }

    }

}
