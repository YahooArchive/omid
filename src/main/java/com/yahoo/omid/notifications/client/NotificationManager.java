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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TTransportException;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yahoo.omid.notifications.Constants;
import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.notifications.thrift.generated.NotificationReceiverService;

public class NotificationManager extends UntypedActor {

    private static final Log logger = LogFactory.getLog(NotificationManager.class);

    private Map<String, ActorRef> registeredObservers;

    private final ExecutorService notificatorAcceptorExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Notificator").build());
       
    /**
     * @param registeredObservers
     * 
     */
    public NotificationManager(Map<String, ActorRef> registeredObservers) {
        this.registeredObservers = registeredObservers;
    }
    
    /*
     * Start Thrift server 
     */
    @Override
    public void preStart() {
        logger.info("Starting Notification. Manager");
        // Initialize Thrift server
        notificatorAcceptorExecutor.execute(new NotificationAcceptor(this.getSelf()));
        logger.info("Notification Manager started");
    }

    @Override
    public void onReceive(Object msg) {
        if (msg instanceof Notification) {
            Notification notification = (Notification) msg;
//            String table = new String(notification.getTable());
//            String rowKey = new String(notification.getRowKey());
//            String columnFamily = new String(notification.getColumnFamily());
//            String column = new String(notification.getColumn());
//            logger.trace("Notification for observer " +
//            notification.getObserver() + " on RowKey " + rowKey
//            + " in Table " + table + " CF " + columnFamily + " C " + column);
            ActorRef obsRef = registeredObservers.get(notification.getObserver());
            if (obsRef != null) {
                obsRef.tell(notification);
            } else {
                logger.error("Observer " + notification.getObserver() + " does not exists!!!");
            }
        } else {
            unhandled(msg);
        }
    }

    /*
     * Stop Thrift server 
     */
    @Override
    public void postStop() {
        logger.info("Stopping Notification Manager");
        notificatorAcceptorExecutor.shutdownNow();
        logger.info("Notification Manager stopped");
    }

    private class NotificationAcceptor implements Runnable, NotificationReceiverService.Iface {

        private TServer server;
        private ActorRef notificationManager;
        
        public NotificationAcceptor(ActorRef notificationManager) {
            this.notificationManager = notificationManager;
        }

        @Override
        public void run() {
            try {
                TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(Constants.THRIFT_SERVER_PORT);
                NotificationReceiverService.Processor<NotificationAcceptor> processor = new NotificationReceiverService.Processor<NotificationAcceptor>(
                        this);
                server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(processor));
                logger.trace("Starting Thrift server on " + Constants.THRIFT_SERVER_PORT);
                server.serve();
            } catch (TTransportException e) {
                e.printStackTrace();
            } finally {
                server.stop();
                logger.trace("Thrift server stopped");
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * com.yahoo.omid.notifications.thrift.generated.ObserverContainer.Iface
         * #update(com.yahoo.omid.notifications.thrift.generated.Notification)
         */
        @Override
        public void notify(Notification notification) throws TException {
            notificationManager.tell(notification);
            // logger.trace("Notification " + notification + " sent to notification manager!!!!");
            // Thread.currentThread().sleep(1000);
        }
    }

}
