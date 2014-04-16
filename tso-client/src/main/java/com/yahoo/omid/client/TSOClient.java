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

package com.yahoo.omid.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.BaseConfiguration;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.yahoo.omid.tso.RowKey;
import com.yahoo.omid.proto.TSOProto;

import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

import com.yahoo.omid.util.StateMachine.State;
import com.yahoo.omid.util.StateMachine.Event;
import com.yahoo.omid.util.StateMachine.Fsm;
import com.yahoo.omid.util.StateMachine.FsmImpl;
import com.yahoo.omid.util.StateMachine.UserEvent;

import com.codahale.metrics.MetricRegistry;

/**
 * Communication endpoint for TSO clients.
 *
 */
public class TSOClient {
    private static final Logger LOG = LoggerFactory.getLogger(TSOClient.class);
    public static final String TSO_HOST_CONFKEY = "tso.host";
    public static final String TSO_PORT_CONFKEY = "tso.port";

    private ChannelFactory factory;
    private ClientBootstrap bootstrap;
    private Fsm fsm;

    private final int max_retries;
    private final int retry_delay_ms; // ignored for now
    private final InetSocketAddress addr;
    private final MetricRegistry metrics;

    public static class ConnectionException extends IOException {}
    public static class AbortException extends Exception {}

    public static class Builder {
        private Configuration conf = new BaseConfiguration();
        private MetricRegistry metrics = new MetricRegistry();
        private String tsoHost = null;
        private int tsoPort = -1;

        public Builder withConfiguration(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public Builder withMetrics(MetricRegistry metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder withTSOHost(String host) {
            this.tsoHost = host;
            return this;
        }

        public Builder withTSOPort(int port) {
            this.tsoPort = port;
            return this;
        }

        public TSOClient build() {
            if (tsoHost != null) {
                conf.setProperty(TSO_HOST_CONFKEY, tsoHost);
            }
            if (tsoPort != -1) {
                conf.setProperty(TSO_PORT_CONFKEY, tsoPort);
            }
            return new TSOClient(conf, metrics);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private TSOClient(Configuration conf, MetricRegistry metrics) {
        this.metrics = metrics;

        // Start client with Nb of active threads = 3 as maximum.
        factory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setNameFormat("tsoclient-boss-%d").build()),
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setNameFormat("tsoclient-worker-%d").build()), 3);
        // Create the bootstrap
        bootstrap = new ClientBootstrap(factory);

        int executorThreads = conf.getInt("tso.executor.threads", 3);

        String host = conf.getString(TSO_HOST_CONFKEY);
        int port = conf.getInt(TSO_PORT_CONFKEY, 1234);
        max_retries = conf.getInt("tso.max_retries", 100);
        retry_delay_ms = conf.getInt("tso.retry_delay_ms", 1000);

        if (host == null) {
            throw new IllegalArgumentException("tso.host missing from configuration");
        }

        addr = new InetSocketAddress(host, port);

        ScheduledExecutorService fsmExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("tsofsm-%d").build());
        fsm = new FsmImpl(fsmExecutor);
        fsm.setInitState(new DisconnectedState());

        ChannelPipeline pipeline = bootstrap.getPipeline();
        pipeline.addLast("lengthbaseddecoder",
                         new LengthFieldBasedFrameDecoder(8*1024, 0, 4, 0, 4));
        pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
        pipeline.addLast("protobufdecoder",
                         new ProtobufDecoder(TSOProto.Response.getDefaultInstance()));
        pipeline.addLast("protobufencoder", new ProtobufEncoder());
        pipeline.addLast("handler", new Handler(fsm));

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("connectTimeoutMillis", 100);
    }

    InetSocketAddress getAddress() {
        return addr;
    }

    public TSOFuture<Long> createTransaction() {
        TSOProto.Request.Builder builder = TSOProto.Request.newBuilder();
        TSOProto.TimestampRequest.Builder tsreqBuilder = TSOProto.TimestampRequest.newBuilder();
        builder.setTimestampRequest(tsreqBuilder.build());
        RequestEvent request = new RequestEvent(builder.build());
        fsm.sendEvent(request);
        return new ForwardingTSOFuture<Long>(request);
    }

    public TSOFuture<Long> commit(long transactionId, RowKey[] rows) {
        TSOProto.Request.Builder builder = TSOProto.Request.newBuilder();
        TSOProto.CommitRequest.Builder commitbuilder = TSOProto.CommitRequest.newBuilder();
        commitbuilder.setStartTimestamp(transactionId);
        for (RowKey r : rows) {
            commitbuilder.addRowHash(r.hashCode());
        }
        builder.setCommitRequest(commitbuilder.build());
        RequestEvent request = new RequestEvent(builder.build());
        fsm.sendEvent(request);
        return new ForwardingTSOFuture<Long>(request);
    }

    private static class ParamEvent<T> implements Event {
        final T param;
        ParamEvent(T param) {
            this.param = param;
        }

        T getParam() {
            return param;
        }
    }

    private static class ErrorEvent extends ParamEvent<Throwable> {
        ErrorEvent(Throwable t) { super(t); }
    }
    private static class ConnectedEvent extends ParamEvent<Channel> {
        ConnectedEvent(Channel c) { super(c); }
    }
    
    private static class CloseEvent extends UserEvent<Void> {}
    private static class ChannelClosedEvent implements Event {}
    private static class RequestEvent extends UserEvent<Long> {
        final TSOProto.Request req;
        RequestEvent(TSOProto.Request req) {
            this.req = req;
        }

        TSOProto.Request getRequest() {
            return req;
        }
    }

    private static class ResponseEvent extends ParamEvent<TSOProto.Response> {
        ResponseEvent(TSOProto.Response r) { super(r); }
    }

    private class BaseState implements State {
        @Override
        public State handleEvent(Fsm fsm, Event e) {
            LOG.error("Unhandled event {} while in state {}", e, this.getClass().getName());
            return this;
        }
    }

    private class DisconnectedState extends BaseState {
        final int retries;

        DisconnectedState(int retries) {
            this.retries = retries;
        }

        DisconnectedState() {
            this.retries = max_retries;
        }

        @Override
        public State handleEvent(final Fsm fsm, Event e) {
            if (e instanceof RequestEvent) {
                if (retries == 0) {
                    LOG.error("Unable to connect after {} retries", max_retries);
                    ((UserEvent)e).error(new ConnectionException());
                    return this;
                }
                fsm.deferUserEvent((UserEvent)e);

                bootstrap.connect(getAddress()).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (!future.isSuccess()) {
                                fsm.sendEvent(new ErrorEvent(future.getCause()));
                            }
                        }
                    });
                return new ConnectingState(retries - 1);
            } else if (e instanceof CloseEvent) {
                ((CloseEvent)e).success(null);
                return this;
            } else {
                super.handleEvent(fsm, e);
                return this;
            }
        }
    }

    private class ConnectingState extends BaseState {
        final int retries;
        ConnectingState(int retries) {
            this.retries = retries;
        }

        @Override
        public State handleEvent(Fsm fsm, Event e) {
            if (e instanceof UserEvent) {
                fsm.deferUserEvent((UserEvent)e);
                return this;
            } else if (e instanceof ConnectedEvent) {
                ConnectedEvent ce = (ConnectedEvent)e;
                return new ConnectedState(ce.getParam());
            } else if (e instanceof ErrorEvent) {
                LOG.error("Error connecting", ((ErrorEvent)e).getParam());
                return new DisconnectedState(retries);
            } else {
                return super.handleEvent(fsm, e);
            }
        }
    }

    private class ConnectedState extends BaseState {
        final Queue<RequestEvent> timestampRequests;
        final Map<Long, RequestEvent> commitRequests;
        final Channel channel;

        ConnectedState(Channel channel) {
            this.channel = channel;
            timestampRequests = new ArrayDeque<RequestEvent>();
            commitRequests = new HashMap<Long, RequestEvent>();
        }

        private void sendRequest(RequestEvent request) {
            TSOProto.Request req = request.getRequest();

            if (req.hasTimestampRequest()) {
                timestampRequests.add(request);
            } else if (req.hasCommitRequest()) {
                TSOProto.CommitRequest commitReq = req.getCommitRequest();
                commitRequests.put(commitReq.getStartTimestamp(), request);
            } else {
                request.error(new IllegalArgumentException("Unknown request type"));
                return;
            }
            ChannelFuture f = channel.write(req);
            f.addListener(new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future) {
                        if (!future.isSuccess()) {
                            fsm.sendEvent(new ErrorEvent(future.getCause()));
                        }
                    }
                });
        }

        private void handleResponse(ResponseEvent response) {
            TSOProto.Response resp = response.getParam();
            if (resp.hasTimestampResponse()) {
                if (timestampRequests.size() == 0) {
                    LOG.warn("Received timestamp response when no requests outstanding");
                    return;
                }
                RequestEvent e = timestampRequests.remove();
                e.success(resp.getTimestampResponse().getStartTimestamp());
            } else if (resp.hasCommitResponse()) {
                long startTimestamp = resp.getCommitResponse().getStartTimestamp();
                RequestEvent e = commitRequests.remove(startTimestamp);
                if (e == null) {
                    LOG.warn("Received commit response for request that doesn't exist."
                            + " Start timestamp: {}", startTimestamp);
                    return;
                }
                if (resp.getCommitResponse().getAborted()) {
                    e.error(new AbortException());
                } else {
                    e.success(resp.getCommitResponse().getCommitTimestamp());
                }
            }
        }

        @Override
        public State handleEvent(Fsm fsm, Event e) {
            if (e instanceof CloseEvent) {
                fsm.deferUserEvent((CloseEvent)e);
                return new ClosingState();
            } else if (e instanceof RequestEvent) {
                sendRequest((RequestEvent)e);
                return this;
            } else if (e instanceof ResponseEvent) {
                handleResponse((ResponseEvent)e);
                return this;
            } else if (e instanceof ErrorEvent) {
                for (RequestEvent r : timestampRequests) {
                    r.error(new ConnectionException());
                }
                for (RequestEvent r : commitRequests.values()) {
                    r.error(new ConnectionException());
                }
                return new ClosingState();
            } else {
                return super.handleEvent(fsm, e);
            }
        }
    }

    private class ClosingState extends BaseState {
        @Override
        public State handleEvent(Fsm fsm, Event e) {
            if (e instanceof UserEvent) {
                fsm.deferUserEvent((UserEvent)e);
                return this;
            } else if (e instanceof ChannelClosedEvent) {
                return new DisconnectedState();
            } else {
                return super.handleEvent(fsm, e);
            }
        }
    }

    private class Handler extends SimpleChannelHandler {
        private Fsm fsm;

        Handler(Fsm fsm) {
            this.fsm = fsm;
        }

        @Override
        public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
            fsm.sendEvent(new ConnectedEvent(e.getChannel()));
        }

        @Override
        public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e)
                throws Exception {
            fsm.sendEvent(new CloseEvent());
        }

        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
                throws Exception {
            fsm.sendEvent(new ChannelClosedEvent());
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            if (e.getMessage() instanceof TSOProto.Response) {
                fsm.sendEvent(new ResponseEvent((TSOProto.Response)e.getMessage()));
            } else {
                LOG.warn("Received unknown message", e.getMessage());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            LOG.error("Error on channel {}", ctx.getChannel(), e.getCause());
            fsm.sendEvent(new ErrorEvent(e.getCause()));
        }
    }
}
