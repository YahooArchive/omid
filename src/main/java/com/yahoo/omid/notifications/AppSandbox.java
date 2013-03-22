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

import static com.yahoo.omid.notifications.ZkTreeUtils.ZK_APP_DATA_NODE;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.ZKPaths;
import com.yahoo.omid.notifications.comm.ZNRecord;
import com.yahoo.omid.notifications.comm.ZNRecordSerializer;
import com.yahoo.omid.notifications.metrics.ServerSideAppMetrics;
import com.yahoo.omid.notifications.thrift.generated.Notification;
import com.yahoo.omid.notifications.thrift.generated.NotificationReceiverService;

public class AppSandbox implements PathChildrenCacheListener {

    private static final Logger logger = Logger.getLogger(AppSandbox.class);

    private CuratorFramework zkClient;

    private ScannerSandbox scannerSandbox;

    private ActorSystem appSandboxActorSystem;

    private PathChildrenCache appsCache;

    private ConcurrentHashMap<String, App> registeredApps = new ConcurrentHashMap<String, App>();

    public AppSandbox(CuratorFramework zkClient, ScannerSandbox scannerSandbox) throws Exception {
        this.zkClient = zkClient;
        this.scannerSandbox = scannerSandbox;
        appSandboxActorSystem = ActorSystem.create("AppSandbox");
        appsCache = new PathChildrenCache(this.zkClient, ZkTreeUtils.getAppsNodePath(), false);
        appsCache.getListenable().addListener(this);
    }

    public void startWatchingAppsNode() throws Exception {
        appsCache.start();
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        String nodeNamePath = event.getData().getPath();
        String appName = ZKPaths.getNodeFromPath(nodeNamePath);

        switch (event.getType()) {
        case CHILD_ADDED: {
            logger.trace("App Node added: " + nodeNamePath);
            createApplication(appName);
            break;
        }

        case CHILD_UPDATED: {
            logger.trace("App Node changed: " + event.getData().getPath());
            break;
        }

        case CHILD_REMOVED: {
            logger.trace("App Node removed: " + event.getData().getPath());
            removeApplication(appName);
            break;
        }
        }
    }

    public void createApplication(String appName) throws Exception {

        synchronized (registeredApps) {
            if (!registeredApps.containsKey(appName)) {
                String appNodePath = ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), appName);
                logger.trace("Getting data from: " + appNodePath);
                byte[] rawData = zkClient.getData().forPath(appNodePath);
                ZNRecord appData = (ZNRecord) new ZNRecordSerializer().deserialize(rawData);
                if (!appName.equals(appData.getId())) {
                    throw new RuntimeException("App data retrieved doesn't corresponds to app: " + appName);
                }
                App app = new App(appName, appData);
                registeredApps.put(appName, app);
                scannerSandbox.registerInterestsFromApplication(app);
                // NOTE: It is not necessary to create the instances. It is triggered automatically by curator
                // through the App.childEvent() callback when constructing the App object (particularly, when
                // registering the interest in the Zk app node)
            }
        }
    }

    private App removeApplication(String appName) throws Exception {
        App removedApp = null;
        synchronized (registeredApps) {
            removedApp = registeredApps.remove(appName);
            if (removedApp != null) {
                scannerSandbox.removeInterestsFromApplication(removedApp);
            } else {
                throw new Exception("App " + appName + " was not registered in AppSanbox");
            }
        }
        return removedApp;
    }

    /**
     * Represents an Application on the server side part of the notification framework. It contains the required
     * meta-data to perform notification to the client side part of the framework
     * 
     */
    class App implements PathChildrenCacheListener {

        private final Logger logger = Logger.getLogger(App.class);

        private String name;

        private ServerSideAppMetrics metrics;

        private ActorRef appInstanceRedirector;

        private PathChildrenCache appsInstanceCache;

        private ConcurrentHashMap<String, ActorRef> instances = new ConcurrentHashMap<String, ActorRef>();
        // private List<ActorRef> instances = new ArrayList<ActorRef>();
        // A mapping between an interest and the observer wanting notifications for changes in that interest
        // Key: The interest as String
        // Value: The AppInstanceNotifer actor in charge of sending notifications to the corresponding app instance
        // TODO now we are considering that only one observer is registered per app
        // Otherwise a List of Observers would be required as a second paramter of the list
        // The Thrift class would need also to be modified
        private ConcurrentHashMap<String, String> interestObserverMap = new ConcurrentHashMap<String, String>();

        public App(String appName, ZNRecord appData) throws Exception {
            this.name = appName;
            this.metrics = new ServerSideAppMetrics(appName);
            appInstanceRedirector = appSandboxActorSystem.actorOf(new Props(new UntypedActorFactory() {
                public UntypedActor create() {
                    return new AppInstanceRedirector();
                }
            }), this.name + "AppInstanceRedirector");
            // Retrieve the obs/interest data from each app data node
            List<String> observersInterests = appData.getListField(ZK_APP_DATA_NODE);
            for (String observerInterest : observersInterests) {
                Iterable<String> tokens = Splitter.on("/").split(observerInterest);
                ArrayList<String> tokenList = Lists.newArrayList(tokens);
                if (tokenList.size() != 2) {
                    throw new RuntimeException("Error extracting data from app node: " + appName);
                }
                String obsName = tokenList.get(0);
                String interest = tokenList.get(1);
                addInterestToObserver(interest, obsName);
            }
            String appPath = ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), this.name);
            appsInstanceCache = new PathChildrenCache(zkClient, appPath, false);
            appsInstanceCache.start();
            appsInstanceCache.getListenable().addListener(this);
        }

        public ActorRef getAppInstanceRedirector() {
            return appInstanceRedirector;
        }

        public Set<String> getInterests() {
            return interestObserverMap.keySet();
        }

        private void addInterestToObserver(String interest, String observer) {
            String result = interestObserverMap.putIfAbsent(interest, observer);
            if (result != null) {
                logger.warn("Other observer than " + observer + " manifested already interest in " + interest);
            }
        }

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            String nodeNamePath = event.getData().getPath();
            String hostnameAndPort = ZKPaths.getNodeFromPath(nodeNamePath);
            switch (event.getType()) {
            case CHILD_ADDED: {
                logger.trace("Instance node added: " + nodeNamePath);
                addInstance(hostnameAndPort);
                break;
            }

            case CHILD_UPDATED: {
                logger.trace("Instance node changed: " + event.getData().getPath());
                break;
            }

            case CHILD_REMOVED: {
                synchronized (instances) {
                    logger.trace("Removing node: " + nodeNamePath + " Instances left: " + instances.size());
                    ActorRef removedAppInstance = instances.remove(hostnameAndPort);
                    if (removedAppInstance != null) {
                        removedAppInstance.tell(PoisonPill.getInstance());
                        if (instances.size() == 0) {
                            String appPath = ZKPaths.makePath(ZkTreeUtils.getAppsNodePath(), name);
                            logger.trace("Zero instances app: " + name + " Removing branch " + appPath);
                            zkClient.delete().forPath(appPath);
                            appInstanceRedirector.tell(PoisonPill.getInstance());
                        }
                        logger.trace("Instance node removed: " + nodeNamePath + " Instances left: " + instances.size());
                    } else {
                        logger.warn("No instance was removed for this node path: " + nodeNamePath);
                    }
                }
                break;
            }
            }
        }

        public void addInstance(String hostnameAndPort) {
            final HostAndPort hp = HostAndPort.fromString(hostnameAndPort);
            ActorRef appNotifierActor = appSandboxActorSystem.actorOf(new Props(new UntypedActorFactory() {
                public UntypedActor create() {
                    return new AppInstanceNotifier(hp.getHostText(), hp.getPort());
                }
            }), name + hostnameAndPort);
            ActorRef result = instances.putIfAbsent(hostnameAndPort, appNotifierActor);
            if (result != null) {
                logger.warn("App instance already running on " + hostnameAndPort);
            }
            logger.trace("Instance " + hostnameAndPort + " added to " + this);
        }

        @Override
        public String toString() {
            return "App [name=" + name + ", instances=" + instances + ", interestObserverMap=" + interestObserverMap
                    + "]";
        }

        /**
         * There's only one actor to redirect messages to application
         * 
         */
        private class AppInstanceRedirector extends UntypedActor {

            int instanceIdx = 0;

            @Override
            public void preStart() {
                logger.trace("App Instance Redirector started");
            }

            @Override
            public void onReceive(Object msg) throws Exception {
                if (msg instanceof UpdatedInterestMsg) {

                    synchronized (instances) {
                        int instanceCount = instances.size();
                        if (instanceCount != 0) {
                            if (instanceIdx == instanceCount) {
                                instanceIdx = 0;
                            }
                            int calculatedIdx = instanceIdx % instanceCount;
                            ActorRef instance = (ActorRef) instances.values().toArray()[calculatedIdx];
                            instance.tell(msg);
                            // logger.trace("App " + name + " sent message " + msg + " to actor " + instance +
                            // " with index " + calculatedIdx);
                            instanceIdx++;
                        } else {
                            logger.warn("App " + name + " has 0 instances to redirect to. Removing actor");
                            getContext().stop(getSelf()); // Stop itself
                        }
                    }

                } else {
                    unhandled(msg);
                }
            }

            @Override
            public void postStop() {
                logger.trace("App Instance Redirector stopped");
            }
        }

        /**
         * There's one actor per application instance deployed
         * 
         */
        private class AppInstanceNotifier extends UntypedActor {

            private String host;
            private int port;
            private TTransport transport;
            private NotificationReceiverService.Client appInstanceClient;

            public AppInstanceNotifier(String host, int port) {
                this.host = host;
                this.port = port;
            }

            @Override
            public void preStart() {
                // Start Thrift communication
                transport = new TFramedTransport(new TSocket(host, port));
                TProtocol protocol = new TBinaryProtocol(transport);
                appInstanceClient = new NotificationReceiverService.Client(protocol);
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

                    if (observer != null) {
                        Interest interest = Interest.fromString(updatedInterest);
                        Notification notification = new Notification(observer, ByteBuffer.wrap(interest
                                .getTableAsHBaseByteArray()), ByteBuffer.wrap(updatedRowKey), // This is the row that
                                                                                              // has been modified
                                ByteBuffer.wrap(interest.getColumnFamilyAsHBaseByteArray()), ByteBuffer.wrap(interest
                                        .getColumnAsHBaseByteArray()));
                        try {
                            appInstanceClient.notify(notification);
                            metrics.notificationSentEvent();
                            // logger.trace("App notifier sent notification " + notification + " to app running on " +
                            // host + ":" + port);
                        } catch (TException te) {
                            logger.warn(name + " app notifier could not send notification " + notification
                                    + " to instance on " + host + ":" + port
                                    + " Destination mailbox may be full or communication channel broken."
                                    + " Trying to re-open transport to instance", te);
                            try {
                                transport.close();
                                transport.open();
                            } catch (TTransportException e) {
                                logger.error(name + " app notifier could not re-open transport to instance on " + host
                                        + ":" + port + " Stopping app notifier", e);
                                getContext().stop(getSelf());
                                throw e;
                            }
                            // TODO Add control flow at the application instance level
                        }
                    } else {
                        logger.warn(name + " app notifier could not send notification to instance on " + host + ":"
                                + port + " because target observer has been removed.");
                    }
                } else {
                    unhandled(msg);
                }
            }

            @Override
            public void postStop() {
                if (transport != null) {
                    transport.close();
                }
                logger.trace("App Notifier stopped");
            }
        }

    }

}
