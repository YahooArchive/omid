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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.utils.ZKPaths;
import com.netflix.curator.utils.ZKPaths.PathAndNode;

public class ZKTreeWatchdog extends AbstractIdleService {

    private static final Log logger = LogFactory.getLog(ZKTreeWatchdog.class);

    private static CuratorFramework zkClient = null;

    // In-memory structures representing the Zk tree
    // Key: Interest (e.g. <table>:<col-fam>:<col> as string) - Value: List<ObserverNames>
    private static Map<String, List<String>> interestsToObservers = Collections.synchronizedMap(new HashMap<String, List<String>>());
    private PathChildrenCache interestsToObserversCache = null;
    private static Map<String, PathChildrenCache> interestsCaches = Collections.synchronizedMap(new HashMap<String, PathChildrenCache>());

    // Key: ObserverName - Value: List<HostWhereObserverIsDeployed>
    private static Map<String, List<String>> observersToHosts = Collections.synchronizedMap(new HashMap<String, List<String>>());
    private PathChildrenCache observersToHostsCache = null;
    private static Map<String, PathChildrenCache> observersCaches = Collections.synchronizedMap(new HashMap<String, PathChildrenCache>());

    private static ScannerManager scannerManager = new ScannerManager(interestsToObservers, observersToHosts);

    // TODO Clean the code below!!!
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.google.common.util.concurrent.AbstractExecutionThreadService#startUp
     * ()
     */
    @Override
    protected void startUp() throws Exception {
        logger.trace("Starting ZK Curator client");
        zkClient = CuratorFrameworkFactory.newClient("localhost:2181", new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        configureZkNotificationTree();
        logger.trace("Finished starting and configuring ZK structure");
    }

    private void configureZkNotificationTree() throws Exception {
        String completeBranchPath = ZKPaths.makePath(Constants.ROOT_NODE, Constants.O2H_NODE);
        logger.trace("Adding listener to O2H Cache with path " + completeBranchPath);
        observersToHostsCache = new PathChildrenCache(zkClient, completeBranchPath, false);
        observersToHostsCache.start();
        addListenerToObserversToHostsNode(observersToHostsCache);

        completeBranchPath = ZKPaths.makePath(Constants.ROOT_NODE, Constants.I2O_NODE);
        logger.trace("Adding listener to I2O Cache with path " + completeBranchPath);
        interestsToObserversCache = new PathChildrenCache(zkClient, completeBranchPath, false);
        interestsToObserversCache.start();
        addListenerToInterestsToObserversNode(interestsToObserversCache);
        
        logger.trace("ZK basic tree created");
    }

    private static void addListenerToObserversToHostsNode(PathChildrenCache cache) {
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                String nodeNamePath = event.getData().getPath();
                String observer = ZKPaths.getNodeFromPath(nodeNamePath);
                synchronized (observersToHosts) {
                    switch (event.getType()) {
                    case CHILD_ADDED: {
                        logger.trace("Node added: " + nodeNamePath);
                        if (!observersToHosts.containsKey(observer)) {
                            observersToHosts.put(observer, new ArrayList<String>());
                            PathChildrenCache cache = new PathChildrenCache(zkClient, nodeNamePath, false);
                            cache.start();
                            addListenerToHostNode(cache);
                            observersCaches.put(observer, cache);
                            List<String> lostChildren = zkClient.getChildren().forPath(nodeNamePath);
                            for (String child : lostChildren) {
                                String childrenNamePath = ZKPaths.makePath(nodeNamePath, child);
                                byte[] data = zkClient.getData().forPath(childrenNamePath);
                                if (!Boolean.parseBoolean(new String(data))) {
                                    logger.trace("MISSING host found " + child);
                                    List<String> hosts = observersToHosts.get(observer);
                                    if (!hosts.contains(child)) {
                                        hosts.add(child);
                                        zkClient.setData().forPath(nodeNamePath, "true".getBytes());
                                    }
                                } else {
                                    logger.trace("Host " + child + " ALREADY CONTROLLED");
                                }
                            }
                            logger.trace("Observer " + observer + " added to in-memory structures");
                        } else {
                            logger.trace("Observer " + observer + " ALREADY added to in-memory structures");
                        }
                        break;
                    }

                    case CHILD_UPDATED: {
                        logger.trace("Node changed: " + event.getData().getPath());
                        break;
                    }

                    case CHILD_REMOVED: {
                        if (observersToHosts.containsKey(observer)) {
                            PathChildrenCache cache = observersCaches.get(observer);
                            Closeables.close(cache, true);
                            observersToHosts.remove(observer);
                            synchronized(interestsToObservers) {
                                Set<String> interests = interestsToObservers.keySet();
                                for (String interest : interests) {
                                    List<String> observers = interestsToObservers.get(interest);
                                    if (observers.contains(observer)) {
                                        String basePath = ZKPaths.makePath(Constants.ROOT_NODE,
                                                Constants.I2O_NODE);
                                        String interestObserverPath = ZKPaths.makePath(interest, observer);
                                        zkClient.delete().forPath(ZKPaths.makePath(basePath, interestObserverPath));
                                    }
                                }
                            }
                            logger.trace("Node removed: " + event.getData().getPath());
                        }

                        break;
                    }
                    }
                }
                traceInMemoryZkStructure();
            }
        };
        cache.getListenable().addListener(listener);
    }
    
    private static void addListenerToHostNode(PathChildrenCache cache) {
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                String nodeNamePath = event.getData().getPath();
                PathAndNode pan = ZKPaths.getPathAndNode(nodeNamePath);
                String host = pan.getNode();
                String observerPath = pan.getPath();
                String observer = ZKPaths.getNodeFromPath(observerPath);
                synchronized(observersToHosts) {
                    List<String> hosts = observersToHosts.get(observer);
                    switch (event.getType()) {
                    case CHILD_ADDED: {
                        logger.trace("Node added: " + nodeNamePath);
                        if (!hosts.contains(host)) {
                            hosts.add(host);
                            zkClient.setData().forPath(nodeNamePath, "true".getBytes());
                            logger.trace("Host " + host + " added to Observer " + observer + " in in-memory structures");
                        } else {
                            logger.trace("Host " + host + " ALREADY added to Observer " + observer
                                    + " in in-memory structures");
                        }
                        break;
                    }
                    case CHILD_UPDATED: {
                        logger.trace("Node ccccccchanged: " + event.getData().getPath());
                        break;
                    }
                    case CHILD_REMOVED: {
                        hosts.remove(host);
                        if (hosts.isEmpty()) {
                            zkClient.delete().forPath(observerPath);
                        }
                        logger.trace("Node rrrrrrremoved: " + event.getData().getPath());
                        break;
                    }
                    }
                }
                traceInMemoryZkStructure();
            }
        };
        cache.getListenable().addListener(listener);
    }
    
    private static void addListenerToInterestsToObserversNode(PathChildrenCache cache) {
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                String nodeNamePath = event.getData().getPath();
                String interest = ZKPaths.getNodeFromPath(nodeNamePath);
                synchronized (interestsToObservers) {
                    switch (event.getType()) {
                    case CHILD_ADDED: {
                        logger.trace("Node added: " + nodeNamePath);
                        if (!interestsToObservers.containsKey(interest)) {
                            interestsToObservers.put(interest, new ArrayList<String>());
                            scannerManager.addScannerContainer(interest);
                            PathChildrenCache cache = new PathChildrenCache(zkClient, nodeNamePath, false);
                            cache.start();
                            addListenerToObserverNode(cache);
                            interestsCaches.put(interest, cache);
                            List<String> lostChildren = zkClient.getChildren().forPath(nodeNamePath);
                            for (String child : lostChildren) {
                                String childrenNamePath = ZKPaths.makePath(nodeNamePath, child);
                                byte[] data = zkClient.getData().forPath(childrenNamePath);
                                if (!Boolean.parseBoolean(new String(data))) {
                                    logger.trace("MISSINGs observer found " + child);
                                    List<String> observers = interestsToObservers.get(interest);
                                    if (!observers.contains(child)) {
                                        observers.add(child);
                                        zkClient.setData().forPath(ZKPaths.makePath(nodeNamePath, child),
                                                "true".getBytes());
                                    }
                                } else {
                                    logger.trace("Observer " + child + " ALREADY CONTROLLEDs");
                                }
                            }
                            logger.trace("Interest " + interest + " added to in-memory structures");
                        } else {
                            logger.trace("Interest " + interest + " ALREADY added to in-memory structures");
                        }
                        break;
                    }

                    case CHILD_UPDATED: {
                        logger.trace("Node changed: " + event.getData().getPath());
                        break;
                    }

                    case CHILD_REMOVED: {
                        if (interestsToObservers.containsKey(interest)) {
                            scannerManager.removeScannerContainer(interest);
                            PathChildrenCache cache = interestsCaches.get(interest);
                            Closeables.close(cache, true);
                            interestsToObservers.remove(interest);
                            logger.trace("Node removed: " + event.getData().getPath());
                        }
                        break;
                    }
                    }
                }
                traceInMemoryZkStructure();
            }
        };
        cache.getListenable().addListener(listener);
    }

    private static void addListenerToObserverNode(PathChildrenCache cache) {
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                String nodeNamePath = event.getData().getPath();
                PathAndNode pan = ZKPaths.getPathAndNode(nodeNamePath);
                String observer = pan.getNode();
                String interestPath = pan.getPath();
                String interest = ZKPaths.getNodeFromPath(interestPath);
                synchronized (interestsToObservers) {
                    List<String> observers = interestsToObservers.get(interest);
                    switch (event.getType()) {
                    case CHILD_ADDED: {
                        logger.trace("Node added: " + nodeNamePath);
                        if (!observers.contains(observer)) {
                            observers.add(observer);
                            zkClient.setData().forPath(nodeNamePath, "true".getBytes());
                            logger.trace("Observer " + observer + " to Interest " + interest
                                    + " in in-memory structures");
                        } else {
                            logger.trace("Observer " + observer + " ALREADY added to Interest " + interest
                                    + " in in-memory structures");
                        }
                        break;
                    }
                    case CHILD_UPDATED: {
                        logger.trace("Node ccccccchanged: " + event.getData().getPath());
                        break;
                    }

                    case CHILD_REMOVED: {
                        observers.remove(observer);
                        if (observers.isEmpty()) {
                            zkClient.delete().forPath(interestPath);
                        }
                        logger.trace("Node rrrrrrremoved: " + event.getData().getPath());
                        break;
                    }
                    }
                }                
                traceInMemoryZkStructure();
            }
        };
        cache.getListenable().addListener(listener);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.google.common.util.concurrent.AbstractExecutionThreadService#shutDown
     * ()
     */
    @Override
    protected void shutDown() throws Exception {
        logger.info("Stopping Scanner Container");
        Closeables.close(interestsToObserversCache, true);
        Closeables.close(observersToHostsCache, true);
        Closeables.close(zkClient, true);
        logger.info("Scanner Container stopped");
    }
    
    /**
     * Just for tracing purposes
     */
    private static void traceInMemoryZkStructure() {
        logger.trace("Current interests: " + interestsToObservers);
        logger.trace("Current observers: " + observersToHosts);
    }
}
