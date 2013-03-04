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

import static com.yahoo.omid.notifications.ZkTreeUtils.ZK_APP_DATA_NODE;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;

import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.ZKPaths;
import com.yahoo.omid.notifications.Constants;
import com.yahoo.omid.notifications.Interest;
import com.yahoo.omid.notifications.NotificationException;
import com.yahoo.omid.notifications.ZkTreeUtils;
import com.yahoo.omid.notifications.comm.ZNRecord;
import com.yahoo.omid.notifications.comm.ZNRecordSerializer;

public class DeltaOmid implements IncrementalApplication {
    
    private static final Logger logger = Logger.getLogger(DeltaOmid.class);
    
    private final CuratorFramework zkClient;
    private final String name;
    private final String zkAppInstancePath;
    private final ActorSystem appObserverSystem;
    
    // The main structure shared by the InterestRecorder and NotificiationListener services in order to register and
    // notify observers
    // Key: The name of a registered observer
    // Value: The TransactionalObserver infrastructure that delegates on the implementation of the ObserverBehaviour
    private final Map<String, ActorRef> registeredObservers = new HashMap<String, ActorRef>();    
    
    public static class AppBuilder {
        // Required parameters
        private final String appName;
        private final List<Observer> observers = new ArrayList<Observer>();
        // Optional parameters - initialized to default values
        private String conf = "localhost:2181"; // TODO Transform this into a real configuration element

        public AppBuilder(String appName) {
            this.appName = appName;
        }
        
        public AppBuilder setConfiguration(String conf) {
            this.conf = conf;
            return this; 
        }
        
        public AppBuilder addObserver(Observer observer) { 
            this.observers.add(observer);
            return this;
        }
        
        public IncrementalApplication build() throws NotificationException {
            return new DeltaOmid(this);
        }
    }

    private DeltaOmid(AppBuilder builder) throws NotificationException {
        this.name = builder.appName;
        
        this.appObserverSystem = ActorSystem.create(name + "ObserverSystem");
        List<String> observersInterests = new ArrayList<String>();
        for(final Observer observer : builder.observers) {
            String obsName = observer.getName();
            // Create an observer wrapper and register it in shared table
            ActorRef obsActor = appObserverSystem.actorOf(
                    new Props(new UntypedActorFactory() {
                        public UntypedActor create() {
                            return new ObserverWrapper(observer);
                        }
                    }), obsName);
            registeredObservers.put(obsName, obsActor);
            List<Interest> interests = observer.getInterests();
            if(interests == null || interests.size() == 0) {
                logger.warn("Observer " + obsName + " doesn't have interests, so it will never be notified");
                continue;
            }
            for(Interest interest : interests) {
                observersInterests.add(obsName + "/" + interest.toZkNodeRepresentation());
            }
        }
        // Create the notification manager for notifying the app observers
        this.appObserverSystem.actorOf(new Props(new UntypedActorFactory() {
            public UntypedActor create() {
                return new NotificationManager(registeredObservers);
            }
        }), "NotificationManager");
        // Finally register the app in the ZK tree
        this.zkClient = CuratorFrameworkFactory.newClient(builder.conf, new ExponentialBackoffRetry(3000, 3));
        this.zkClient.start();
        ZNRecord zkData = new ZNRecord(name);
        zkData.putListField(ZK_APP_DATA_NODE, observersInterests);
        try {
            String zkAppPath = createZkSubBranch(ZkTreeUtils.getAppsNodePath(), name, false);
            this.zkClient.setData().inBackground().forPath(zkAppPath, new ZNRecordSerializer().serialize(zkData));
            String instanceName = InetAddress.getLocalHost().getHostAddress() + ":" + Constants.THRIFT_SERVER_PORT;
            this.zkAppInstancePath = createZkSubBranch(zkAppPath, instanceName, true);
        } catch (Exception e) {
            throw new NotificationException(e);
        }
        logger.trace("App instance created: " + this);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws IOException {
        try {
            zkClient.delete().forPath(zkAppInstancePath);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {            
            appObserverSystem.shutdown();
            Closeables.closeQuietly(zkClient);
        }
        logger.trace("App instance finished: ");
    }
    
    private String createZkSubBranch(String mainBranchPath, String subBranchPath, boolean ephemeral) throws Exception {
        String completeBranchPath = ZKPaths.makePath(mainBranchPath, subBranchPath);
        Stat s = zkClient.checkExists().forPath(completeBranchPath);
        if (s == null) {
            if(ephemeral) {
                return zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(completeBranchPath);
            } else {
                return zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(completeBranchPath);                
            }
        }
        return completeBranchPath;
    }

    @Override
    public String toString() {
        return "DeltaOmid [name=" + name + ", zkAppInstancePath=" + zkAppInstancePath + ", appObserverSystem="
                + appObserverSystem + ", registeredObservers=" + registeredObservers + "]";
    }
    
    
}
