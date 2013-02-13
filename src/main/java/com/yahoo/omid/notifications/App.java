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
package com.yahoo.omid.notifications;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;

import com.google.common.net.HostAndPort;
import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.notifications.thrift.generated.NotificationReceiverService;

/**
 * Represents an Application on the server side part of the notification framework.
 * It contains the required meta-data to perform notification to the client side part of the framework
 *
 */
public class App {

    private static final Logger logger = Logger.getLogger(App.class);
    
    private ActorSystem appActorSystem;
    
    private String name;
    
    // A mapping between a hostname containing an app instance and its corresponding notification actor
    // Key: The hostname:port combination of the app to be notified as String
    // Value: The AppInstanceNotifer actor in charge of sending notifications to the corresponding app instance
    private ConcurrentHashMap<String, ActorRef> hostsRunning = new ConcurrentHashMap<String, ActorRef>();
    // A mapping between an interest and the observer wanting notifications for changes in that interest
    // Key: The interest as String
    // Value: The AppInstanceNotifer actor in charge of sending notifications to the corresponding app instance
    // TODO now we are considering that only one observer is registered per app
    // Otherwise a List of Observers would be required as a second paramter of the list
    // The Thrift class would need also to be modified
    private ConcurrentHashMap<String, String> interestObserverMap = new ConcurrentHashMap<String, String>();
    
    private AtomicLong counter = new AtomicLong(0);

    
    public App(ActorSystem appActorSystem, String appName) {
        this.appActorSystem = appActorSystem;
        this.name = appName;
    }

    public boolean isInstanceRunningInHost(String hostnameAndPort) {
        return hostsRunning.containsKey(hostnameAndPort);
    }

    public boolean isInstanceRunningInHost(String hostname, int port) {
        String hostAndPortKey = hostname + ":" + String.valueOf(port);
        return isInstanceRunningInHost(hostAndPortKey);
    }
    
    public void addHostRunningApp(String hostnameAndPort) {
        final HostAndPort hp = HostAndPort.fromString(hostnameAndPort);
        // Register observer in shared table
        ActorRef appNotifierActor = this.appActorSystem.actorOf(
                new Props(new UntypedActorFactory() {
                    public UntypedActor create() {
                        return new AppInstanceNotifier(hp.getHostText(), hp.getPort());
                    }
                }), name + hostnameAndPort + counter.getAndIncrement());
        logger.trace("COUNTER " + counter.get());
        ActorRef result = hostsRunning.putIfAbsent(hostnameAndPort, appNotifierActor);
        if(result != null) {            
            logger.warn("App instance already running on " + hostnameAndPort);
        }
    }

    public ArrayList<ActorRef> getHostsRunningInstances() {
        return new ArrayList<ActorRef>(hostsRunning.values());
    }
    
    public void removeHostRunningApp(String hostname) {        
        hostsRunning.remove(hostname);        
    }
    
    public int hostRunningAppCount() {        
        return hostsRunning.size();        
    }
    
    public void addInterestToObserver(String interest, String observer) {
        String result = interestObserverMap.putIfAbsent(interest, observer);
        if(result != null) {            
            logger.warn("Observer " + observer + " already manifested interest in " + interest);
        }
    }

    public void removeInterestFromObserver(String interest, String observer) {
        interestObserverMap.remove(interest, observer);
    }

    /**
     * Added to be used when the ZK node representing an interest is removed
     */
    public void removeInterest(String interest) {
        interestObserverMap.remove(interest);
    }
    
    /**
     * There's one actor per application instance deployed
     *
     */
    private class AppInstanceNotifier extends UntypedActor {
        
        private String host;
        private int port;
        private TTransport transport;
        private NotificationReceiverService.Client client;

        
        public AppInstanceNotifier(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void preStart() {
            // Start Thrift communication
            transport = new TFramedTransport(new TSocket(host, port));
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new NotificationReceiverService.Client(protocol);
            try {
                transport.open();
            } catch (TTransportException e) {
                e.printStackTrace();
                getContext().stop(getSelf());
            }
            logger.trace("App Notifier started");
        }
        
        @Override
        public void onReceive(Object msg) throws Exception {
            if (msg instanceof UpdatedInterestMsg) {
                String updatedInterest = ((UpdatedInterestMsg) msg).interest;
                byte[] updatedRowKey = ((UpdatedInterestMsg) msg).rowKey;
                
                String observer = interestObserverMap.get(updatedInterest);
                
                if(observer != null) {
                    Interest interest = Interest.fromString(updatedInterest);
                    Notification notification = new Notification(observer, 
                            ByteBuffer.wrap(interest.getTableAsHBaseByteArray()),
                            ByteBuffer.wrap(updatedRowKey), // This is the row that has been modified
                            ByteBuffer.wrap(interest.getColumnFamilyAsHBaseByteArray()),
                            ByteBuffer.wrap(interest.getColumnAsHBaseByteArray()));
                    try {
    
                        client.notify(notification);
                        // logger.trace("App notifier sent notification " + notification + " to app running on " + host + ":" + port);
                    } catch (Exception e) {
                        logger.error("App notifier could not send notification " + notification + " to app running on "
                                + host + ":" + port);
                    }
                } else {
                    logger.warn("App notifier could not send notification because target observer has been removed.");
                }
            } else {
                unhandled(msg);
            }
        }
        
        @Override
        public void postStop() {
            if(transport != null) {
                transport.close();
            }
            logger.trace("App Notifier stopped");
        }
    }
}
