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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import akka.actor.ActorRef;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.notifications.Constants;
import com.yahoo.omid.notifications.NotificationException;
import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.notifications.thrift.generated.NotificationReceiverService;

public class NotificationManager {

    private static final Log logger = LogFactory.getLog(NotificationManager.class);

    private static final long TIMEOUT = 3;

    private Map<String, ActorRef> registeredObservers;
    
    private final ExecutorService notificatorAcceptorExecutor;
       
    public NotificationManager(String appName, Map<String, ActorRef> registeredObservers) {
        this.registeredObservers = registeredObservers;
        this.notificatorAcceptorExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(
                appName + "Notificator").build());
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
                TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(Constants.THRIFT_SERVER_PORT);
                NotificationReceiverService.Processor<NotificationDispatcher> processor = new NotificationReceiverService.Processor<NotificationDispatcher>(
                        this);
                server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
                logger.trace("Starting Thrift server on " + Constants.THRIFT_SERVER_PORT);
                server.serve();
            } catch (TTransportException e) {
                e.printStackTrace();
            } finally {
                stop();
            }
        }

        private void stop() {
            server.stop();
            logger.trace("Thrift server stopped");
        }

        /*
         * Thrift generated
         */
        @Override
        public void notify(Notification notification) throws TException {
            ActorRef obsRef = registeredObservers.get(notification.getObserver());
            if (obsRef != null) {
                obsRef.tell(notification);
            } else {
                logger.error("Observer " + notification.getObserver() + " can not be notified");
            }
        }
    }

}
