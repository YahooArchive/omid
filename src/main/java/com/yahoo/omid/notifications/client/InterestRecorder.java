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

import java.net.InetAddress;
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
import com.google.common.util.concurrent.AbstractIdleService;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.ZKPaths;
import com.yahoo.omid.notifications.Constants;
import com.yahoo.omid.notifications.Interest;

public class InterestRecorder extends AbstractIdleService {

    private static final Logger logger = Logger.getLogger(InterestRecorder.class);

    private ActorSystem observersSystem;
    private Map<String, ActorRef> registeredObservers;
    
    private CuratorFramework zkClient = null;
    
    public InterestRecorder(ActorSystem observersSystem, Map<String, ActorRef> registeredObservers) {
        this.observersSystem = observersSystem;
        this.registeredObservers = registeredObservers;
    }

    /**
     * The public method available used by the applications to register the
     * interest of observers in a particular column The registration process is
     * done through Zk.
     * 
     * @param obsName
     * @param obsBehaviour
     * @param system 
     * @param table
     * @param columnFamily
     * @param column
     * @throws Exception
     */
    public void registerObserverInterest(final String obsName, final ObserverBehaviour obsBehaviour, Interest interest) throws Exception {

        String clientHost = InetAddress.getLocalHost().getHostAddress();
        // Register first in which host is the observer (in /onf/oh branch)...
        createZkSubBrach(ZKPaths.makePath(Constants.ROOT_NODE, Constants.O2H_NODE), ZKPaths.makePath(obsName, clientHost), true);
        // ...and then what is its interest (in /onf/io branch)
        createZkSubBrach(ZKPaths.makePath(Constants.ROOT_NODE, Constants.I2O_NODE), ZKPaths.makePath(interest.toZkNodeRepresentation(), obsName), false);
                
        // Register observer in shared table
        ActorRef obsActor = observersSystem.actorOf(
                new Props(new UntypedActorFactory() {
                    public UntypedActor create() {
                        return new TransactionalObserver(obsName, obsBehaviour);
                    }
                }), obsName);
        registeredObservers.put(obsName, obsActor);
        
    }

    private void createZkSubBrach(String mainBranchPath, String subBranchPath, boolean ephemeral) throws Exception {
        String completeBranchPath = ZKPaths.makePath(mainBranchPath, subBranchPath);
        Stat s = zkClient.checkExists().forPath(completeBranchPath);
        if (s == null) {
            if(ephemeral) {
                zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(completeBranchPath);
            } else {
                zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(completeBranchPath);                
            }
            logger.trace(subBranchPath + " added to " + mainBranchPath);
        }
    }
    
    /**
     * TODO Apply DRY
     * 
     * @param obsName
     * @param interest
     * @throws Exception
     */
    public void deregisterObserverInterest(String obsName, Interest interest) throws Exception {
        
        // De-register from ZK where is the observer. This will trigger the observer de-registration
        // from interests
        String clientHost = InetAddress.getLocalHost().getHostAddress();
        String basePath = ZKPaths.makePath(Constants.ROOT_NODE, Constants.O2H_NODE);
        String path = ZKPaths.makePath(obsName, clientHost);
        logger.trace("De-registering observer and its hosts from ZK");
        zkClient.delete().forPath(ZKPaths.makePath(basePath, path));
        // De-register observer in shared table
        registeredObservers.remove(obsName);
    }
    
    /*
     * Create connection with Zookeeper
     * 
     * (non-Javadoc)
     * 
     * @see com.google.common.util.concurrent.AbstractIdleService#startUp()
     */
    @Override
    protected void startUp() throws Exception {
        logger.info("Starting Interest Recorder");
        logger.trace("Starting ZK Curator client");
        zkClient = CuratorFrameworkFactory.newClient("localhost:2181", new ExponentialBackoffRetry(3000, 3));
        zkClient.start();
        logger.info("Interest Recorder started");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.util.concurrent.AbstractIdleService#shutDown()
     */
    @Override
    protected void shutDown() throws Exception {
        logger.info("Stopping Interest Recorder");
        Closeables.closeQuietly(zkClient);
        logger.info("Interest Recorder stopped");
    }
}
