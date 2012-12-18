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
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.common.util.concurrent.AbstractIdleService;
import com.yahoo.omid.notifications.Constants;
import com.yahoo.omid.notifications.Interest;

public class InterestRecorder extends AbstractIdleService implements Watcher {

    private static final Logger logger = Logger.getLogger(InterestRecorder.class);

    private Map<String, TransactionalObserver> registeredObservers;
    
    private ZooKeeper zk;
    private CountDownLatch zkStartedCdl = new CountDownLatch(1);

    public InterestRecorder(Map<String, TransactionalObserver> registeredObservers, String zkConn, Configuration hHbaseConfig) {
        this.registeredObservers = registeredObservers;
    }

    /**
     * The public method available used by the applications to register the
     * interest of observers in a particular column The registration process is
     * done through Zk.
     * 
     * TODO Apply DRY
     * 
     * @param obs
     * @param table
     * @param columnFamily
     * @param column
     * @throws Exception
     */
    public void register(TransactionalObserver obs, Interest interest) throws Exception {

        registerObserversAndHosts(obs);
        registerInterestsAndObservers(obs, interest);
                
        // Register observer in shared table
        registeredObservers.put(obs.getName(), obs);
        
    }

    /**
     * Register in ZK in which server is located each observer
     * @param obs
     * @throws KeeperException
     * @throws InterruptedException
     * @throws UnknownHostException
     */
    private void registerObserversAndHosts(TransactionalObserver obs) throws KeeperException, InterruptedException,
            UnknownHostException {
        String zkObserverNodePath = new StringBuffer(Constants.NOTIF_OBSERVERS).append("/").append(obs.getName()).toString();
        Stat s = zk.exists(zkObserverNodePath, false);
        if (s == null) {
            zk.create(zkObserverNodePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.trace(zkObserverNodePath + " registered in observers");
        }
        String zkHostNode = new StringBuffer(InetAddress.getLocalHost().getHostAddress()).toString();
        String zkHostSubNodePath = new StringBuffer(zkObserverNodePath).append("/").append(zkHostNode).toString();
        s = zk.exists(zkHostSubNodePath, false);
        if (s == null) {
            zk.create(zkHostSubNodePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.trace(zkHostSubNodePath + " registered in observers");
        }
    }

    /**
     * Register in ZK in which column has an observer interest
     * @param obs
     * @param interest
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void registerInterestsAndObservers(TransactionalObserver obs, Interest interest) throws KeeperException,
            InterruptedException {
        String zkInterestNode = interest.toZkNodeRepresentation();
        String zkInterestNodePath = new StringBuffer(Constants.NOTIF_INTERESTS).append("/").append(zkInterestNode).toString();
        Stat s = zk.exists(zkInterestNodePath, false);
        if (s == null) {
            zk.create(zkInterestNodePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.trace(zkInterestNodePath + " registered in interests");
        }
        String zkObserverSubNodePath = new StringBuffer(zkInterestNodePath).append("/").append(obs.getName()).toString();
        s = zk.exists(zkObserverSubNodePath, false);
        if (s == null) {
            zk.create(zkObserverSubNodePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.trace(zkObserverSubNodePath + " registered in interests");
        }
    }
    
    /**
     * TODO Apply DRY
     * 
     * @param obs
     * @param interest
     * @throws Exception
     */
    public void deregister(TransactionalObserver obs, Interest interest) throws Exception {
        logger.trace("Deregistering observer");
        String zkInterestNode = interest.toZkNodeRepresentation();
        String zkInterestNodePath = new StringBuffer(Constants.NOTIF_INTERESTS).append("/").append(zkInterestNode).toString();
        Stat s = zk.exists(zkInterestNodePath, false);
        if (s == null) {
            throw new IllegalArgumentException("ZK node " + zkInterestNodePath + " not exists");
        }
        // TODO Implement properly without removing all the observers below
        List<String> observers = zk.getChildren(zkInterestNodePath, false);
        for(String observer : observers) {
            zk.delete(new StringBuffer(zkInterestNodePath).append("/").append(observer).toString(), -1);
        }
        zk.delete(zkInterestNodePath, -1);
        
        String zkObserverNode = obs.getName();
        String zkObserverNodePath = new StringBuffer(Constants.NOTIF_OBSERVERS).append("/").append(zkObserverNode).toString();
        s = zk.exists(zkObserverNodePath, false);
        if (s == null) {
            throw new IllegalArgumentException("ZK node " + zkObserverNodePath + " not exists");
        }
        // TODO Implement properly without removing all the hosts below
        List<String> hosts = zk.getChildren(zkObserverNodePath, false);
        for(String host : hosts) {
            zk.delete(new StringBuffer(zkObserverNodePath).append("/").append(host).toString(), -1);
        }
        zk.delete(zkObserverNodePath, -1);

        // Deregister observer from shared table
        registeredObservers.remove(obs.getName());
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
        logger.trace("Starting ZK");
        zk = new ZooKeeper("localhost:2181", 3000, this);
        zkStartedCdl.await();
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
        zk.close();
        logger.info("Interest Recorder stopped");
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
     */
    @Override
    public void process(WatchedEvent event) {
        logger.trace("New ZK event received: " + event);
        zkStartedCdl.countDown();
    }
}
