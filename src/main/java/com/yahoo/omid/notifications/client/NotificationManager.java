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
package com.yahoo.omid.notifications.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.dispatch.MessageQueueAppendFailedException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.notifications.NotificationException;
import com.yahoo.omid.notifications.metrics.ClientSideAppMetrics;
import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.notifications.thrift.generated.NotificationReceiverService;
import com.yahoo.omid.notifications.thrift.generated.ObserverOverloaded;

public class NotificationManager {

    private static final Logger logger = LoggerFactory.getLogger(NotificationManager.class);

    private static final long TIMEOUT = 3;

    private final IncrementalApplication app;

    private final ClientSideAppMetrics metrics;

    private final ExecutorService notificatorAcceptorExecutor;

    public NotificationManager(IncrementalApplication app, ClientSideAppMetrics metrics) {
        this.app = app;
        this.metrics = metrics;
        this.notificatorAcceptorExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(
                app.getName() + "-Notificator-%d").build());
    }

    public void start() throws NotificationException {
        notificatorAcceptorExecutor.execute(new NotificationDispatcher());
    }

    public void stop() {
        notificatorAcceptorExecutor.shutdown();
        try {
            notificatorAcceptorExecutor.awaitTermination(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class NotificationDispatcher implements Runnable, NotificationReceiverService.Iface {

        private TServer server;

        @Override
        public void run() {
            try {
                TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(app.getPort());
                NotificationReceiverService.Processor<NotificationDispatcher> processor = new NotificationReceiverService.Processor<NotificationDispatcher>(
                        this);
                server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
                logger.info("App " + app.getName() + " listening for notifications on port " + app.getPort());
                server.serve();
            } catch (TTransportException e) {
                e.printStackTrace();
            } finally {
                stop();
            }
        }

        private void stop() {
            if (server != null) {
                server.stop();
                logger.info("App " + app.getName() + " stopped listening for notifications on port " + app.getPort());
            }
        }

        /*
         * Thrift generated
         */
        @Override
        public void notify(Notification notification) throws ObserverOverloaded, TException {
            metrics.notificationReceivedEvent();
            ActorRef obsRef = app.getRegisteredObservers().get(notification.getObserver());
            if (obsRef != null) {
                try {
                    obsRef.tell(notification);
                } catch (MessageQueueAppendFailedException e) {
                    logger.warn("Cannot place msg " + notification + " in Observer's queue");
                    throw new ObserverOverloaded();
                }
            } else {
                logger.error("Observer " + notification.getObserver() + " cannot be notified");
            }
        }
    }

}
