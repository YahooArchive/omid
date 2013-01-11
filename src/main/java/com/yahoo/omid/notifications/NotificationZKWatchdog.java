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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.util.concurrent.AbstractIdleService;

public class NotificationZKWatchdog extends AbstractIdleService implements Watcher {

    private static final Log logger = LogFactory.getLog(NotificationZKWatchdog.class);

    private ZooKeeper zk;
    private CountDownLatch zkStartedCdl = new CountDownLatch(1);

    // In-memory structures representing the Zk tree
    // Key: TableName - Value: List<ObserverNames>
    private Map<String, List<String>> interestsToObservers = new HashMap<String, List<String>>();
    
    // Key: ObserverName - Value: List<HostWhereObserverIsDeployed>
    private Map<String, List<String>> observersToHosts = new HashMap<String, List<String>>();
    
    // Key: Table where the scanner container is performing scanning
    // Value: The scanner container that executes the scanner threads
    private Map<String, ScannerContainer> scanners = new HashMap<String, ScannerContainer>();

    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.util.concurrent.AbstractExecutionThreadService#startUp()
     */
    @Override
    protected void startUp() throws Exception {
        logger.info("Starting Scanner Manager");
        // Create connection with zk, download and register
        logger.trace("Starting ZK");
        zk = new ZooKeeper("localhost:2181", 3000, this);
        zkStartedCdl.await();
        try {
            configureZkNotificationTree();
        } catch (Exception e) {
            logger.error("Error configuring ZooKeeper");
            zk.close();
            throw e;
        }
        // Populate in-memory structures with current observers and in which hosts are running
        List<String> interests = zk.getChildren(Constants.NOTIF_INTERESTS, false);
        for (String interest : interests) {
            List<String> observers = zk.getChildren(Constants.NOTIF_INTERESTS + "/" + interest, false);
            // TODO add the proper Watchers to each interest node
            interestsToObservers.put(interest, observers);
        }
        List<String> observers = zk.getChildren(Constants.NOTIF_OBSERVERS, false);
        for (String observer : observers) {
            List<String> hosts = zk.getChildren(Constants.NOTIF_OBSERVERS + "/" + observer, false);
            // TODO add the proper Watchers to each host node
            observersToHosts.put(observer, hosts);
        }
        
        // For each interest, add Scanner. NOTE. This is done AFTER the previous maps have been filled properly
        for (String interest : interests) {            
            ScannerContainer scannerContainer = new ScannerContainer(interest, interestsToObservers.get(interest), interestsToObservers, observersToHosts);
            scannerContainer.start();
            scanners.put(interest, scannerContainer);
        }
        
        // Register interests to observe how the children change on these nodes
        zk.getChildren(Constants.NOTIF_INTERESTS, new InterestNodeWatcher());
        zk.getChildren(Constants.NOTIF_OBSERVERS, new ObserverNodeWatcher());
        traceInMemoryZkStructure();
        logger.trace("Finished starting and configuring ZK: " + zk);
        logger.info("Scanner Manager started");
    }
    
    /**
     * Just for tracing purposes
     */
    private void traceInMemoryZkStructure() {
        logger.trace("Current interests: " + interestsToObservers);
        logger.trace("Current observers: " + observersToHosts);
    }

    /**
     * 
     */
    private void configureZkNotificationTree() throws Exception {
        // Create ZK node name if not exits
        if (zk.exists(Constants.NOTIF_ROOT, false) == null) {
            zk.create(Constants.NOTIF_ROOT, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create(Constants.NOTIF_INTERESTS, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create(Constants.NOTIF_OBSERVERS, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            if (zk.exists(Constants.NOTIF_INTERESTS, false) == null) {
                zk.create(Constants.NOTIF_INTERESTS, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zk.exists(Constants.NOTIF_OBSERVERS, false) == null) {
                zk.create(Constants.NOTIF_OBSERVERS, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
        logger.trace("ZK path infrastructure created");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.util.concurrent.AbstractExecutionThreadService#shutDown()
     */
    @Override
    protected void shutDown() throws Exception {
        logger.info("Stopping Scanner Container");
        zk.close();
        logger.info("Scanner Container stopped");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     */
    @Override
    public void process(WatchedEvent event) {
        logger.trace("New ZK event received: " + event);
        zkStartedCdl.countDown();
    }

    private void addObserverToScanner(String interest, String observer) throws Exception {
        ScannerContainer scannerContainer = scanners.get(interest);
        if(scannerContainer == null) {
            scannerContainer = new ScannerContainer(interest, observer, interestsToObservers, observersToHosts);
            scannerContainer.start();
            scanners.put(interest, scannerContainer);
            logger.trace("Scanner and interest " + interest + " added to in-memory structures for  observer " + observer);
        } else {
            scannerContainer.addObserver(interest, observer);
            logger.trace("Observer " + observer + " added to Scanner and interest " + interest);
        }
    }
    
    // TODO Reduce duplicated code in the 4 classes below
    
    private class InterestNodeWatcher extends NodeStructureWatcher {

        public InterestNodeWatcher() {
            super(Constants.NOTIF_INTERESTS, new ArrayList<String>(interestsToObservers.keySet()));
        }

        @Override
        public synchronized void performActionOnAddedNodes(String parentNode, List<String> addedNodes) throws Exception {
            for (String node : addedNodes) {
                if (!interestsToObservers.containsKey(node)) {
                    String zkNodePath = new StringBuffer(parentNode).append("/").append(node).toString();
                    interestsToObservers.put(node, new ArrayList<String>());
                    List<String> lostChildren = zk.getChildren(zkNodePath, new InterestObserverNodeWatcher(zkNodePath), null);
                    if(lostChildren.size() > 0) {
                        logger.trace("Lost children found in InterestNode Watcher. Node : " + node +  " " + lostChildren);
                        interestsToObservers.get(node).addAll(lostChildren);
                        for(String observer : lostChildren) {
                            addObserverToScanner(node, observer);
                        }                        
                    }
                } else {
                    logger.trace("Interest " + node + " already into in-memory structures");
                }
            }
        }

        @Override
        public synchronized void performActionOnDeletedNodes(String parentNode, List<String> deletedNodes) throws Exception {
            for (String node : deletedNodes) {
                if (!interestsToObservers.containsKey(node)) {
                    throw new IllegalArgumentException("Interest " + node + " doesn't exists");
                }
                interestsToObservers.remove(node);
                ScannerContainer scannerContainer = scanners.get(node);
                scannerContainer.stop();
                scanners.remove(node);
                logger.trace("Interest " + node + " removed from in-memory structures ");
            }
        }
        
    }
    
    private class InterestObserverNodeWatcher extends NodeStructureWatcher {

        public InterestObserverNodeWatcher(String nodePath) {
            super(nodePath, interestsToObservers.get(nodePath.split("/")[nodePath.split("/").length - 1]));
        }

        @Override
        public synchronized void performActionOnAddedNodes(String parentNode, List<String> addedNodes) throws Exception {
            String[] path = parentNode.split("/");
            String interestNode = path[path.length - 1];
            List<String> observers = interestsToObservers.get(interestNode);
            for(String node: addedNodes) {
                if (!observers.contains(node)) {
                    observers.add(node);
                    addObserverToScanner(interestNode, node);
                    logger.trace("The observer " + node + " was added to the list of the " + interestNode + " interest");
                } else {
                    logger.trace("The observer " + node + " was already in the list of the  " + interestNode + " interest");
                }
            }
        }

        // TODO When removing observers, this method does not takes into account the number references in the cluster!!! SOLVE THIS
        @Override
        public synchronized void performActionOnDeletedNodes(String parentNode, List<String> deletedNodes) throws Exception {
            String[] path = parentNode.split("/");
            String interestNode = path[path.length - 1];
            List<String> observers = interestsToObservers.get(interestNode);
            for(String node: deletedNodes) {
                if (observers.contains(node)) {
                    observers.remove(node);
                    logger.trace("The observer " + node + " was removed from the list of the " + interestNode + " interest");
                } else {
                    logger.trace("The observer " + node + " could not be removed. It was not in the list of the " + interestNode + " interest");
                }
            }
        }

    }

    
    private class ObserverNodeWatcher extends NodeStructureWatcher {

        public ObserverNodeWatcher() {
            super(Constants.NOTIF_OBSERVERS, new ArrayList<String>(observersToHosts.keySet()));
        }
        
        @Override
        public synchronized void performActionOnAddedNodes(String parentNode, List<String> addedNodes) throws Exception {
            for (String node : addedNodes) {
                if (!observersToHosts.containsKey(node)) {
                    String zkNodePath = new StringBuffer(parentNode).append("/").append(node).toString();
                    observersToHosts.put(node, new ArrayList<String>());
                    List<String> lostChildren = zk.getChildren(zkNodePath, new ObserverHostsNodeWatcher(zkNodePath), null);
                    if(lostChildren.size() > 0) {
                        logger.trace("Lost children found in Observer Node Watcher. Node : " + node +  " " + lostChildren);                        
                        observersToHosts.get(node).addAll(lostChildren);
                    }                    
                    logger.trace("Host " + node + " added to in-memory structures");
                } else {
                    logger.trace("Host " + node + " already into in-memory structures");
                }
            }    
        }

        @Override
        public synchronized void performActionOnDeletedNodes(String parentNode, List<String> deletedNodes) throws Exception {
            for (String node : deletedNodes) {
                if (!observersToHosts.containsKey(node)) {
                    throw new IllegalArgumentException("Host " + node + " doesn't exists");
                }
                observersToHosts.remove(node);
                logger.trace("Host " + node + " removed from in-memory structures ");
            }
        }
        
    }
    
    private class ObserverHostsNodeWatcher extends NodeStructureWatcher {

        public ObserverHostsNodeWatcher(String nodePath) {
            super(nodePath, observersToHosts.get(nodePath.split("/")[nodePath.split("/").length - 1]));
        }

        @Override
        public synchronized void performActionOnAddedNodes(String parentNode, List<String> addedNodes) throws Exception {
            String[] path = parentNode.split("/");
            String interestNode = path[path.length - 1];
            List<String> hosts = observersToHosts.get(interestNode);
            for(String node: addedNodes) {
                if (!hosts.contains(node)) {
                    hosts.add(node);
                    logger.trace("The host " + node + " was added to the list of the " + interestNode + " observer");
                } else {
                    logger.trace("The host " + node + " was already in the list of the " + interestNode + " observer");
                }
            }
        }

        // TODO When removing hosts, this method does not takes into account the number references in the cluster!!! SOLVE THIS
        @Override
        public synchronized void performActionOnDeletedNodes(String parentNode, List<String> deletedNodes) throws Exception {
            String[] path = parentNode.split("/");
            String interestNode = path[path.length - 1];
            List<String> observers = observersToHosts.get(interestNode);
            for(String node: deletedNodes) {
                if (observers.contains(node)) {
                    observers.remove(node);
                    logger.trace("The host " + node + " was removed from the list of the " + interestNode + " observer");
                } else {
                    logger.trace("The observer " + node + " could not be removed. It was not in the list of the " + interestNode + " observer");
                }
            }
        }

    }
    
    private abstract class NodeStructureWatcher implements Watcher {

        private String observableNodePath;
        private List<String> currentChildren;

        public NodeStructureWatcher(String observableNodePath, List<String> currentChildren) {
            this.observableNodePath = observableNodePath;
            this.currentChildren = currentChildren;
        }
        
        public abstract void performActionOnAddedNodes(String parentNode, List<String> addedNodes) throws Exception;
        
        public abstract void performActionOnDeletedNodes(String parentNode, List<String> deletedNodes) throws Exception;

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
         */
        @Override
        public synchronized void process(WatchedEvent event) {
            logger.trace("New ZK event received: " + event);

            EventType eventType = event.getType();
            String targetNode = event.getPath();
            if (targetNode != null && targetNode.startsWith(observableNodePath) && (eventType != null)) {

                try {
                    if (eventType.equals(EventType.NodeChildrenChanged)) {

                        List<String> newElems = new ArrayList<String>();
                        List<String> missingElems = new ArrayList<String>();
                        // Every time a watch is processed you need to
                        // re-register for the next one.
                        List<String> children = zk.getChildren(observableNodePath, this);
                        for (String child : children) {
                            if (currentChildren.contains(child) == false) {
                                newElems.add(child);
                                logger.trace("Data node " + child + " was added");
                            }
                        }

                        for (String currentSequence : currentChildren) {
                            if (children.contains(currentSequence) == false) {
                                missingElems.add(currentSequence);
                                logger.trace("Data node " + currentSequence + " was removed");
                            }
                        }

                        performActionOnAddedNodes(observableNodePath, newElems);
                        performActionOnDeletedNodes(observableNodePath, missingElems);
                        traceInMemoryZkStructure();
                    } else if (eventType.equals(EventType.NodeDeleted)) {
                        logger.error("Node " + observableNodePath + " was deleted");
                        if(targetNode.equals(Constants.NOTIF_ROOT) ||
                                targetNode.equals(Constants.NOTIF_INTERESTS) ||
                                targetNode.equals(Constants.NOTIF_OBSERVERS)) {
                            logger.trace("Recreating...");
                            configureZkNotificationTree();
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }
    }

}
