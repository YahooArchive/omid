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

package com.yahoo.omid.tsoclient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.proto.TSOProto;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.CommitTable.Client;
import com.yahoo.omid.committable.NullCommitTable;

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
    public static final String REQUEST_MAX_RETRIES_CONFKEY = "request.max-retries";
    public static final String REQUEST_TIMEOUT_IN_MS_CONFKEY = "request.timeout-ms";
    public static final int DEFAULT_TSO_PORT = 54758;
    public static final int DEFAULT_TSO_MAX_REQUEST_RETRIES = 5;
    public static final int DEFAULT_REQUEST_TIMEOUT_MS = 5000; // 5 secs

    private ChannelFactory factory;
    private ClientBootstrap bootstrap;
    private final ScheduledExecutorService fsmExecutor;
    Fsm fsm;

    private final int requestTimeoutMs;
    private final int requestMaxRetries;
    private final int retry_delay_ms; // ignored for now
    private final InetSocketAddress addr;
    private final MetricRegistry metrics;

    public static class ConnectionException extends IOException {}

    public static class ClosingException extends Exception {
    }

    public static class ServiceUnavailableException extends Exception {
    }

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

        String host = conf.getString(TSO_HOST_CONFKEY);
        int port = conf.getInt(TSO_PORT_CONFKEY, DEFAULT_TSO_PORT);
        requestTimeoutMs = conf.getInt(REQUEST_TIMEOUT_IN_MS_CONFKEY, DEFAULT_REQUEST_TIMEOUT_MS);
        requestMaxRetries = conf.getInt(REQUEST_MAX_RETRIES_CONFKEY, DEFAULT_TSO_MAX_REQUEST_RETRIES);
        retry_delay_ms = conf.getInt("tso.retry_delay_ms", 1000);

        if (host == null) {
            throw new IllegalArgumentException("tso.host missing from configuration");
        }

        addr = new InetSocketAddress(host, port);

        fsmExecutor = Executors.newSingleThreadScheduledExecutor(
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
        RequestEvent request = new RequestEvent(builder.build(), requestMaxRetries);
        fsm.sendEvent(request);
        return new ForwardingTSOFuture<Long>(request);
    }

    public TSOFuture<Long> commit(long transactionId, Set<? extends CellId> cells) {
        TSOProto.Request.Builder builder = TSOProto.Request.newBuilder();
        TSOProto.CommitRequest.Builder commitbuilder = TSOProto.CommitRequest.newBuilder();
        commitbuilder.setStartTimestamp(transactionId);
        for (CellId cell : cells) {
            commitbuilder.addCellId(cell.getCellId());
        }
        builder.setCommitRequest(commitbuilder.build());
        RequestEvent request = new RequestEvent(builder.build(), requestMaxRetries);
        fsm.sendEvent(request);
        return new ForwardingTSOFuture<Long>(request);
    }

    public TSOFuture<Void> close() {
        CloseEvent closeEvent = new CloseEvent();
        fsm.sendEvent(closeEvent);
        closeEvent.addListener(new Runnable() {
                @Override
                public void run() {
                    fsmExecutor.shutdown();
                }
            }, fsmExecutor);
        return new ForwardingTSOFuture<Void>(closeEvent);
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

    private static class TimestampRequestTimeoutEvent implements Event {
    }

    private static class CommitRequestTimeoutEvent implements Event {
        final long startTimestamp;

        public CommitRequestTimeoutEvent(long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }
    }

    private static class RequestEvent extends UserEvent<Long> {
        TSOProto.Request req;
        int retriesLeft;

        RequestEvent(TSOProto.Request req, int retriesLeft) {
            this.req = req;
            this.retriesLeft = retriesLeft;
        }

        TSOProto.Request getRequest() {
            return req;
        }

        void setRequest(TSOProto.Request request) {
            this.req = request;
        }

        int getRetriesLeft() {
            return retriesLeft;
        }

        void decrementRetries() {
            retriesLeft--;
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
            this.retries = requestMaxRetries;
        }

        @Override
        public State handleEvent(final Fsm fsm, Event e) {
            if (e instanceof RequestEvent) {
                if (retries == 0) {
                    LOG.error("Unable to connect after {} retries", requestMaxRetries);
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
                factory.releaseExternalResources();
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

    class ConnectedState extends BaseState {
        final Queue<RequestEvent> timestampRequests;
        final Map<Long, RequestEvent> commitRequests;
        final Channel channel;

        final ScheduledExecutorService timeoutExecutor = Executors
                .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("tso-client-timeout")
                        .build());

        ConnectedState(Channel channel) {
            this.channel = channel;
            timestampRequests = new ArrayDeque<RequestEvent>();
            commitRequests = new HashMap<Long, RequestEvent>();
        }

        private void sendRequest(final Fsm fsm, RequestEvent request) {
            TSOProto.Request req = request.getRequest();

            final Event timeoutEvent;
            if (req.hasTimestampRequest()) {
                timestampRequests.add(request);
                timeoutEvent = new TimestampRequestTimeoutEvent();
            } else if (req.hasCommitRequest()) {
                TSOProto.CommitRequest commitReq = req.getCommitRequest();
                commitRequests.put(commitReq.getStartTimestamp(), request);
                timeoutEvent = new CommitRequestTimeoutEvent(commitReq.getStartTimestamp());
            } else {
                timeoutEvent = null;
                request.error(new IllegalArgumentException("Unknown request type"));
                return;
            }
            ChannelFuture f = channel.write(req);
            if (requestTimeoutMs > 0) {
                timeoutExecutor.schedule(new Runnable() {
                        @Override
                        public void run() {
                            fsm.sendEvent(timeoutEvent);
                        }
                    }, requestTimeoutMs, TimeUnit.MILLISECONDS);
            }
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
                    LOG.debug("Received timestamp response when no requests outstanding");
                    return;
                }
                RequestEvent e = timestampRequests.remove();
                e.success(resp.getTimestampResponse().getStartTimestamp());
            } else if (resp.hasCommitResponse()) {
                long startTimestamp = resp.getCommitResponse().getStartTimestamp();
                RequestEvent e = commitRequests.remove(startTimestamp);
                if (e == null) {
                    LOG.debug("Received commit response for request that doesn't exist."
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
            if (e instanceof TimestampRequestTimeoutEvent) {
                if (!timestampRequests.isEmpty()) {
                    queueRetryOrError(fsm, timestampRequests.remove());
                }
                return this;
            } else if (e instanceof CommitRequestTimeoutEvent) {
                long startTimestamp = ((CommitRequestTimeoutEvent) e).getStartTimestamp();
                if (commitRequests.containsKey(startTimestamp)) {
                    queueRetryOrError(fsm, commitRequests.remove(startTimestamp));
                }
                return this;
            } else if (e instanceof CloseEvent) {
                timeoutExecutor.shutdownNow();
                closeChannelAndErrorRequests();
                fsm.deferUserEvent((CloseEvent) e);
                return new ClosingState();
            } else if (e instanceof RequestEvent) {
                sendRequest(fsm, (RequestEvent) e);
                return this;
            } else if (e instanceof ResponseEvent) {
                handleResponse((ResponseEvent)e);
                return this;
            } else if (e instanceof ErrorEvent) {
                timeoutExecutor.shutdownNow();
                handleError(fsm);
                return new ClosingState();
            } else {
                return super.handleEvent(fsm, e);
            }
        }

        private void handleError(Fsm fsm) {
            while (timestampRequests.size() > 0) {
                queueRetryOrError(fsm, timestampRequests.remove());
            }
            for (long l : commitRequests.keySet()) {
                queueRetryOrError(fsm, commitRequests.remove(l));
            }
            channel.close();
        }

        private void queueRetryOrError(Fsm fsm, RequestEvent e) {
            if (e.getRetriesLeft() > 0) {
                e.decrementRetries();
                if (e.getRequest().hasCommitRequest()) {
                    TSOProto.CommitRequest commitRequest = e.getRequest().getCommitRequest();
                    if (!commitRequest.getIsRetry()) { // Create a new retry for the commit request
                        TSOProto.Request.Builder builder = TSOProto.Request.newBuilder();
                        TSOProto.CommitRequest.Builder commitBuilder = TSOProto.CommitRequest.newBuilder();
                        commitBuilder.mergeFrom(commitRequest);
                        commitBuilder.setIsRetry(true);
                        builder.setCommitRequest(commitBuilder.build());
                        e.setRequest(builder.build());
                    }
                }
                fsm.sendEvent(e);
            } else {
                e.error(new ServiceUnavailableException());
            }
        }

        private void closeChannelAndErrorRequests() {
            channel.close();
            for (RequestEvent r : timestampRequests) {
                r.error(new ClosingException());
            }
            for (RequestEvent r : commitRequests.values()) {
                r.error(new ClosingException());
            }
        }
    }

    private class ClosingState extends BaseState {
        @Override
        public State handleEvent(Fsm fsm, Event e) {
            if (e instanceof TimestampRequestTimeoutEvent
                    || e instanceof CommitRequestTimeoutEvent
                    || e instanceof ErrorEvent
                    || e instanceof ResponseEvent) {
                // Ignored. They will be retried or errored
                return this;
            } else if (e instanceof UserEvent) {
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
            fsm.sendEvent(new ErrorEvent(new ConnectionException()));
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
